//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 VMware, Inc. or its affiliates.
//
//	@filename:
//		CXformGbAggWithMDQA2Join.h
//
//	@doc:
//		Transform a GbAgg with multiple distinct qualified aggregates (MDQAs)
//		to a join tree with single DQA leaves
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformGbAggWithMDQA2Join_H
#define GPOPT_CXformGbAggWithMDQA2Join_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"
#include "gpopt/base/CUtils.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformGbAggWithMDQA2Join
//
//	@doc:
//		Transform a GbAgg with multiple distinct qualified aggregates (MDQAs)
//		to a join tree with single DQA leaves
//
//---------------------------------------------------------------------------
class CXformGbAggWithMDQA2Join : public CXformExploration
{
private:
	// hash map between expression and a column reference
	using ExprToColRefMap =
		CHashMap<CExpression, CColRef, CExpression::HashValue, CUtils::Equals,
				 CleanupRelease<CExpression>, CleanupNULL<CColRef>>;

	static CExpression *PexprMDQAs2Join(CMemoryPool *mp, CExpression *pexpr);

	// expand GbAgg with multiple distinct aggregates into a join of single distinct
	// aggregates
	static CExpression *PexprExpandMDQAs(CMemoryPool *mp, CExpression *pexpr);

	static CExpression *PexprTupSplitMDQAs(CMemoryPool *mp, CExpression *pexpr);

	static CExpression *PexprTupSplitAggs(CMemoryPool *mp, CExpression *pexpr);

	static void ExtractDistinctCols(CMemoryPool *mp, CColumnFactory *col_factory, CMDAccessor *md_accessor,
									CExpression *pexpr, CExpressionArray *pdrgpexprChildPrEl,
									ExprToColRefMap *phmexprcr,
									CColRefArray **ppdrgpcrArgDQA);  // output: array of distinct aggs arguments

	static CColRef *PcrAggFuncArgument(CMemoryPool *mp, CMDAccessor *md_accessor,
								CColumnFactory *col_factory,
								CExpression *pexprArg,
								CExpressionArray *pdrgpexprChildPrEl);
	static CExpression *PexprTupSplitAggregations(
								CMemoryPool *mp, CExpression *pexprRelational,
								CExpressionArray *pdrgpexprPrElFirstStage,
								CExpressionArray *pdrgpexprPrElThirdStage, CColRefArray *pdrgpcrArgDQA,
								CColRefArray *pdrgpcrLastStage, CColRefArray *dqaexprs, CColRef *aggexprid);

	// main transformation function driver
	static CExpression *PexprTransform(CMemoryPool *mp, CExpression *pexpr);

public:
	CXformGbAggWithMDQA2Join(const CXformGbAggWithMDQA2Join &) = delete;

	// ctor
	explicit CXformGbAggWithMDQA2Join(CMemoryPool *mp);

	// dtor
	~CXformGbAggWithMDQA2Join() override = default;

	// ident accessors
	EXformId
	Exfid() const override
	{
		return ExfGbAggWithMDQA2Join;
	}

	// return a string for xform name
	const CHAR *
	SzId() const override
	{
		return "CXformGbAggWithMDQA2Join";
	}

	// compute xform promise for a given expression handle
	EXformPromise Exfp(CExpressionHandle &exprhdl) const override;

	// actual transform
	void Transform(CXformContext *, CXformResult *,
				   CExpression *) const override;

	// return true if xform should be applied only once
	BOOL IsApplyOnce() override;

};	// class CXformGbAggWithMDQA2Join

}  // namespace gpopt

#endif	// !GPOPT_CXformGbAggWithMDQA2Join_H

// EOF
