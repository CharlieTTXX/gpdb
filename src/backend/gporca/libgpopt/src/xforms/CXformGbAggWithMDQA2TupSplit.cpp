//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2024 VMware, Inc.
//
//	@filename:
//		CXformGbAggWithMDQA2TupSplit.cpp
//
//	@doc:
//		Transform a GbAgg with multiple distinct qualified aggregates (MDQAs)
//		to a join tree with single DQA leaves
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformGbAggWithMDQA2TupSplit.h"

#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"

#include "gpopt/base/CKeyCollection.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CLogicalGbAgg.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/md/IMDAggregate.h"
#include "gpopt/operators/CScalarValuesList.h"
#include "naucrates/md/IMDTypeInt4.h"
#include "gpopt/operators/CLogicalTupSplit.h"
#include "gpopt/operators/CScalarProjectList.h"
#include "gpopt/operators/CScalarAggExprId.h"

using namespace gpmd;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAggWithMDQA2TupSplit::CXformGbAggWithMDQA2TupSplit
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformGbAggWithMDQA2TupSplit::CXformGbAggWithMDQA2TupSplit(CMemoryPool *mp)
	: CXformExploration(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalGbAgg(mp),
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternTree(mp)),  // relational child
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternTree(mp))  // scalar project list
			  ))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAggWithMDQA2TupSplit::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformGbAggWithMDQA2TupSplit::Exfp(CExpressionHandle &exprhdl) const
{
	CAutoMemoryPool amp;

	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(exprhdl.Pop());

	if (COperator::EgbaggtypeGlobal == popAgg->Egbaggtype() &&
		exprhdl.DeriveHasMultipleDistinctAggs(1))
	{
		return CXform::ExfpHigh;
	}

	return CXform::ExfpNone;
}

CColRef *
CXformGbAggWithMDQA2TupSplit::PcrAggFuncArgument(CMemoryPool *mp, CMDAccessor *md_accessor,
								   CColumnFactory *col_factory,
								   CExpression *pexprArg,
								   CExpressionArray *pdrgpexprChildPrEl)
{
	GPOS_ASSERT(nullptr != pexprArg);
	GPOS_ASSERT(nullptr != pdrgpexprChildPrEl);

	if (COperator::EopScalarIdent == pexprArg->Pop()->Eopid())
	{
		return (const_cast<CColRef *>(
			CScalarIdent::PopConvert(pexprArg->Pop())->Pcr()));
	}

	CScalar *popScalar = CScalar::PopConvert(pexprArg->Pop());
	// computed argument to the input
	const IMDType *pmdtype = md_accessor->RetrieveType(popScalar->MdidType());
	CColRef *pcrAdditionalGrpCol =
		col_factory->PcrCreate(pmdtype, popScalar->TypeModifier());

	pexprArg->AddRef();
	CExpression *pexprPrElNew =
		CUtils::PexprScalarProjectElement(mp, pcrAdditionalGrpCol, pexprArg);

	pdrgpexprChildPrEl->Append(pexprPrElNew);

	return pcrAdditionalGrpCol;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformGbAggWithMDQA2TupSplit::ExtractDistinctCols
//
//	@doc:
//		Extract arguments of distinct aggs
//
//---------------------------------------------------------------------------
void
CXformGbAggWithMDQA2TupSplit::ExtractDistinctCols(
	CMemoryPool *mp, CColumnFactory *col_factory, CMDAccessor *md_accessor,
	CExpression *pexpr, CExpressionArray *pdrgpexprChildPrEl,
	ExprToColRefMap *phmexprcr,
	CColRefArray **ppdrgpcrArgDQA  // output: array of distinct aggs arguments
)
{
	GPOS_ASSERT(nullptr != pdrgpexprChildPrEl);
	GPOS_ASSERT(nullptr != ppdrgpcrArgDQA);
	GPOS_ASSERT(nullptr != phmexprcr);

	const ULONG arity = pexpr->Arity();
	BOOL hasNonSplittableAgg = false;

	// use a set to deduplicate distinct aggs arguments
	CColRefSet *pcrsArgDQA = GPOS_NEW(mp) CColRefSet(mp);
	ULONG ulDistinct = 0;
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprPrEl = (*pexpr)[ul];

		// get the scalar child of the project element
		CExpression *pexprAggFunc = (*pexprPrEl)[0];
		CScalarAggFunc *popScAggFunc =
			CScalarAggFunc::PopConvert(pexprAggFunc->Pop());
		hasNonSplittableAgg =
			!md_accessor->RetrieveAgg(popScAggFunc->MDId())->IsSplittable();

		// if an agg fucntion is missing a combine function, then such an agg is
		// called non splittable. Non splittable aggs cannot participate in multi-phase DQAs
		// We do not track missing combine functions per DQA so we cannot have some
		// as single phase and some as multiple phases.
		if (hasNonSplittableAgg)
		{
			break;
		}

		if (popScAggFunc->IsDistinct())
		{
			// CScalarValuesList
			for (ULONG ul = 0; ul < (*pexprAggFunc)[EaggfuncIndexArgs]->Arity();
				 ul++)
			{
				CExpression *pexprArg =
					(*(*pexprAggFunc)[EaggfuncIndexArgs])[ul];
				GPOS_ASSERT(nullptr != pexprArg);
				CColRef *pcrDistinctCol = phmexprcr->Find(pexprArg);
				if (nullptr == pcrDistinctCol)
				{
					ulDistinct++;

					// get the column reference of the DQA argument
					pcrDistinctCol =
						PcrAggFuncArgument(mp, md_accessor, col_factory,
										   pexprArg, pdrgpexprChildPrEl);

					// insert into the map between the expression representing the DQA argument
					// and its column reference
					pexprArg->AddRef();
					BOOL fInserted GPOS_ASSERTS_ONLY =
						phmexprcr->Insert(pexprArg, pcrDistinctCol);
					GPOS_ASSERT(fInserted);

					// add the distinct column to the set of distinct columns
					pcrsArgDQA->Include(pcrDistinctCol);
				}
			}
		}
	}

	if (!hasNonSplittableAgg)
	{
		*ppdrgpcrArgDQA = pcrsArgDQA->Pdrgpcr(mp);
	}
	else
	{
		// failed to find a single DQA, or agg is defined as non-splittable
		*ppdrgpcrArgDQA = nullptr;
	}
	pcrsArgDQA->Release();
}

CExpression *
CXformGbAggWithMDQA2TupSplit::PexprTupSplitMDQAs(CMemoryPool *mp, CExpression *pexpr)
{
	// extract components
	CExpression *pexprRel = (*pexpr)[0];
	CExpression *pexprPrL = (*pexpr)[1];

	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(pexpr->Pop());
	CColRefArray *pdrgpcrGlobal = popAgg->Pdrgpcr();

	ExprToColRefMap *phmexprcr = GPOS_NEW(mp) ExprToColRefMap(mp);
	CExpressionArray *pdrgpexprChildPrEl = GPOS_NEW(mp) CExpressionArray(mp);
	CColRefArray *pdrgpcrArgDQA = nullptr;

	ExtractDistinctCols(mp, col_factory, md_accessor, pexprPrL, pdrgpexprChildPrEl, phmexprcr, &pdrgpcrArgDQA);

	if (0 < pdrgpexprChildPrEl->Size())
	{
		pexprRel->AddRef();

		// computed columns referred to in the DQA
		CExpression *pexprChildProject = CUtils::PexprLogicalProject(
			mp, pexprRel,
			GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp),
									 pdrgpexprChildPrEl),
			true /*fNewComputedCol*/
		);
		pexprRel = pexprChildProject;
	}

	// array of project elements for the local (first), intermediate
	// (second) and global (third) aggregate operator
	CExpressionArray *pdrgpexprPrElFirstStage =
		GPOS_NEW(mp) CExpressionArray(mp);
	CExpressionArray *pdrgpexprPrElLastStage =
		GPOS_NEW(mp) CExpressionArray(mp);

	const IMDType *pmdtype = md_accessor->PtMDType<IMDTypeInt4>();
	CColRef *pcrAggexprid = col_factory->PcrCreate(pmdtype, default_type_modifier);

	ULONG aggexprid = 1;
	CColRefArray *dqaexprs = GPOS_NEW(mp) CColRefArray(mp);;
	const ULONG arity = pexprPrL->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprPrEl = (*pexprPrL)[ul];
		CScalarProjectElement *popScPrEl =
			CScalarProjectElement::PopConvert(pexprPrEl->Pop());

		// get the scalar aggregate function
		CExpression *pexprAggFunc = (*pexprPrEl)[0];
		CScalarAggFunc *popScAggFunc =
			CScalarAggFunc::PopConvert(pexprAggFunc->Pop());

		if (popScAggFunc->IsDistinct())
		{
			// create a new "non-distinct" version of the original aggregate function
			popScAggFunc->MDId()->AddRef();
			popScAggFunc->GetArgTypes()->AddRef();
			CScalarAggFunc *popScAggFuncNew = CUtils::PopAggFunc(
				mp, popScAggFunc->MDId(),
				GPOS_NEW(mp)
					CWStringConst(mp, popScAggFunc->PstrAggFunc()->GetBuffer()),
				false /* is_distinct */, EaggfuncstageGlobal /*eaggfuncstage*/,
				false /* fSplit */, nullptr /* pmdidResolvedReturnType */,
				EaggfunckindNormal, popScAggFunc->GetArgTypes(),
				popScAggFunc->FRepSafe(), aggexprid);

			aggexprid++;

			CExpressionArray *pdrgpexprChildren =
				GPOS_NEW(mp) CExpressionArray(mp);

			CExpressionArray *pdrgpexprArgs = GPOS_NEW(mp) CExpressionArray(mp);
			for (ULONG ul = 0; ul < (*pexprAggFunc)[0]->Arity(); ul++)
			{
				CExpression *pexprArg = (*(*pexprAggFunc)[0])[ul];
				CColRef *pcrDistinctCol = phmexprcr->Find(pexprArg);
				GPOS_ASSERT(nullptr != pcrDistinctCol);

				pdrgpexprArgs->Append(
					CUtils::PexprScalarIdent(mp, pcrDistinctCol));
			
				dqaexprs->Append(pcrDistinctCol);
			}

			// agg args
			pdrgpexprChildren->Append(GPOS_NEW(mp) CExpression(
				mp, GPOS_NEW(mp) CScalarValuesList(mp), pdrgpexprArgs));

			// agg direct args
			pdrgpexprChildren->Append(
				GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarValuesList(mp),
										 GPOS_NEW(mp) CExpressionArray(mp)));

			// agg order
			pdrgpexprChildren->Append(
				GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarValuesList(mp),
										 GPOS_NEW(mp) CExpressionArray(mp)));

			// agg distinct
			CExpressionArray *pdrgpexprDirectArgs =
				GPOS_NEW(mp) CExpressionArray(mp);
			for (ULONG ul = 0;
				 ul < (*pexprAggFunc)[EaggfuncIndexDistinct]->Arity(); ul++)
			{
				CExpression *pexprDirectArg =
					(*(*pexprAggFunc)[EaggfuncIndexDistinct])[ul];
				pexprDirectArg->AddRef();
				pdrgpexprDirectArgs->Append(pexprDirectArg);
			}
			pdrgpexprChildren->Append(GPOS_NEW(mp) CExpression(
				mp, GPOS_NEW(mp) CScalarValuesList(mp), pdrgpexprDirectArgs));

			CExpression *pexprPrElGlobal = CUtils::PexprScalarProjectElement(
				mp, popScPrEl->Pcr(),
				GPOS_NEW(mp)
					CExpression(mp, popScAggFuncNew, pdrgpexprChildren));

			pdrgpexprPrElLastStage->Append(pexprPrElGlobal);
		}
		else
		{
			// split the regular aggregate function into multi-level aggregate functions
		}
	}

	// let's construct pdrgpexprPrElFirstStage
	CExpression *pexprProjElem = GPOS_NEW(mp) CExpression(
			mp, GPOS_NEW(mp) CScalarProjectElement(mp, pcrAggexprid),
			GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarAggExprId(mp)));
	pdrgpexprPrElFirstStage->Append(pexprProjElem);

	CExpression *pexprGlobal = PexprTupSplitAggregations(
		mp, pexprRel, pdrgpexprPrElFirstStage,
		pdrgpexprPrElLastStage, pdrgpcrArgDQA, pdrgpcrGlobal,
		dqaexprs, pcrAggexprid
	);

	//maybe construct a function

	return pexprGlobal;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformGbAggWithMDQA2TupSplit::PexprTupSplitAggregations
//
//	@doc:
//		Generate an expression with multi-level aggregation
//
//---------------------------------------------------------------------------
CExpression *
CXformGbAggWithMDQA2TupSplit::PexprTupSplitAggregations(
	CMemoryPool *mp, CExpression *pexprRelational,
	CExpressionArray *pdrgpexprPrElFirstStage,
	CExpressionArray *pdrgpexprPrElThirdStage, CColRefArray *pdrgpcrArgDQA,
	CColRefArray *pdrgpcrLastStage, CColRefArray *dqaexprs, CColRef *aggexprid)
{
	GPOS_ASSERT(nullptr != pexprRelational);
	GPOS_ASSERT(nullptr != pdrgpcrArgDQA);

	CColRefArray *pdrgpcrLocal = CUtils::PdrgpcrExactCopy(mp, pdrgpcrLastStage);
	const ULONG length = pdrgpcrArgDQA->Size();
	GPOS_ASSERT(0 < length);


	// add the distinct column and aggexprid to the group by
	// at the first stage of the multi-level aggregation
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp, pdrgpcrLocal);
	for (ULONG ul = 0; ul < length; ul++)
	{
		CColRef *colref = (*pdrgpcrArgDQA)[ul];
		if (!pcrs->FMember(colref))
		{
			pdrgpcrLocal->Append(colref);
			pcrs->Include(colref);
		}
	}
	pcrs->Release();


	CLogicalGbAgg *popFirstStage = nullptr;
	CLogicalGbAgg *popSecondStage = nullptr;
	CLogicalTupSplit *popTupSplit = nullptr;


	popTupSplit = GPOS_NEW(mp) CLogicalTupSplit(mp, aggexprid, dqaexprs, pdrgpcrLastStage);
	// the local aggregate is responsible for removing duplicates
	pdrgpcrArgDQA->AddRef();

	// add aggexprid to group columns
	pdrgpcrLocal->Append(aggexprid);

	popFirstStage = GPOS_NEW(mp) CLogicalGbAgg(
		mp, pdrgpcrLocal, COperator::EgbaggtypeLocal,
		false /* fGeneratesDuplicates */, pdrgpcrArgDQA, CLogicalGbAgg::EasOthers);
	pdrgpcrLastStage->AddRef();
	popSecondStage = GPOS_NEW(mp) CLogicalGbAgg(
		mp, pdrgpcrLastStage, COperator::EgbaggtypeGlobal, /* egbaggtype */
		CLogicalGbAgg::EasOthers, aggexprid);;

	pexprRelational->AddRef();
	CExpression *pexprTupSplit = GPOS_NEW(mp) CExpression(
		mp, popTupSplit, pexprRelational,
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp),
								 pdrgpexprPrElFirstStage));

	CExpression *pexprFirstStage = GPOS_NEW(mp) CExpression(
		mp, popFirstStage, pexprTupSplit,
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp)));

	CExpression *pexprSecondStage = GPOS_NEW(mp) CExpression(
		mp, popSecondStage, pexprFirstStage,
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp),
								 pdrgpexprPrElThirdStage));

	return pexprSecondStage;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformGbAggWithMDQA2Join::Transform
//
//	@doc:
//		Actual transformation to expand multiple distinct qualified aggregates
//		(MDQAs) to a join tree with single DQA leaves
//
//---------------------------------------------------------------------------
void
CXformGbAggWithMDQA2TupSplit::Transform(CXformContext *pxfctxt,
									CXformResult *pxfres,
									CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(nullptr != pxfres);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	CExpression *pexprResult = PexprTupSplitMDQAs(mp, pexpr);
	if (nullptr != pexprResult)
	{
		pxfres->Add(pexprResult);
	}
}

BOOL
CXformGbAggWithMDQA2TupSplit::IsApplyOnce()
{
	return true;
}
// EOF
