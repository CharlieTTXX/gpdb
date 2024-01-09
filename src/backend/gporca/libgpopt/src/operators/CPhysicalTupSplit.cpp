//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2024 VMware, Inc.
//
//	@filename:
//		CPhysicalTupSplit.cpp
//
//	@doc:
//		Implementation of physical TupSplit operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalTupSplit.h"

#include "gpos/base.h"

#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecRandom.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CScalarIdent.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTupSplit::CPhysicalTupSplit
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalTupSplit::CPhysicalTupSplit(CMemoryPool *mp,
                                     CColRef *pcrAggexprid,
                                     CColRefArray *dqapexprs,
                                     CColRefArray *pdrgpcr)
	: CPhysical(mp),
	  m_aggexprid(pcrAggexprid),
	  m_dqaexprs(dqapexprs),
      m_pdrgpcr(pdrgpcr),
      m_pcrsRequiredLocal(nullptr)
{
	GPOS_ASSERT(nullptr != pcrAggexprid);
	GPOS_ASSERT(nullptr != dqapexprs);
	GPOS_ASSERT(nullptr != pdrgpcr);

	// required columns by local members
	m_pcrsRequiredLocal = GPOS_NEW(mp) CColRefSet(mp);
	m_pcrsRequiredLocal->Include(m_dqaexprs);
	m_pcrsRequiredLocal->Include(m_pdrgpcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTupSplit::~CPhysicalTupSplit
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalTupSplit::~CPhysicalTupSplit()
{
	m_dqaexprs->Release();
    m_pdrgpcr->Release();
    m_pcrsRequiredLocal->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTupSplit::PosRequired
//
//	@doc:
//		Compute required sort columns of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalTupSplit::PosRequired(CMemoryPool *mp,
							CExpressionHandle &,  // exprhdl
							COrderSpec *,		  // posRequired
							ULONG
#ifdef GPOS_DEBUG
								child_index
#endif	// GPOS_DEBUG
							,
							CDrvdPropArray *,  // pdrgpdpCtxt
							ULONG			   // ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);

	// return empty sort order
	return GPOS_NEW(mp) COrderSpec(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalTupSplit::PosDerive(CMemoryPool *,  //mp,
						     CExpressionHandle &exprhdl) const
{
	return PosDerivePassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTupSplit::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalTupSplit::EpetOrder(CExpressionHandle &,	// exprhdl
						  const CEnfdOrder *
#ifdef GPOS_DEBUG
							  peo
#endif	// GPOS_DEBUG
) const
{
	GPOS_ASSERT(nullptr != peo);
	GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

	// always force sort to be on top of split
	return CEnfdProp::EpetUnnecessary;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalTupSplit::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//		we only compute required columns for the relational child;
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalTupSplit::PcrsRequired(CMemoryPool *mp,
							 CExpressionHandle &,  // exprhdl,
							 CColRefSet *pcrsRequired,
							 ULONG
#ifdef GPOS_DEBUG
								 child_index
#endif	// GPOS_DEBUG
							 ,
							 CDrvdPropArray *,	// pdrgpdpCtxt
							 ULONG				// ulOptReq
)
{
	GPOS_ASSERT(0 == child_index);

	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp, *m_pcrsRequiredLocal);

	//TODO we need consider more about here as with-agg is on
	pcrs->Union(pcrsRequired);
	pcrs->Exclude(m_aggexprid);

	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalTupSplit::PdsRequired(CMemoryPool *mp,
							CExpressionHandle &,  // exprhdl,
							CDistributionSpec *,  // pdsInput,
							ULONG
#ifdef GPOS_DEBUG
								child_index
#endif	// GPOS_DEBUG
							,
							CDrvdPropArray *,  // pdrgpdpCtxt
							ULONG			   // ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);

	return GPOS_NEW(mp) CDistributionSpecAny(this->Eopid());
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::PrsRequired
//
//	@doc:
//		Compute required rewindability of the n-th child
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalTupSplit::PrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
							CRewindabilitySpec *prsRequired, ULONG child_index,
							CDrvdPropArray *,  // pdrgpdpCtxt
							ULONG			   // ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);

	return PrsPassThru(mp, exprhdl, prsRequired, child_index);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *
CPhysicalTupSplit::PcteRequired(CMemoryPool *,		   //mp,
							 CExpressionHandle &,  //exprhdl,
							 CCTEReq *pcter,
							 ULONG
#ifdef GPOS_DEBUG
								 child_index
#endif
							 ,
							 CDrvdPropArray *,	//pdrgpdpCtxt,
							 ULONG				//ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);
	return PcterPushThru(pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalTupSplit::FProvidesReqdCols(CExpressionHandle &exprhdl,
								  CColRefSet *pcrsRequired,
								  ULONG	 // ulOptReq
) const
{
	GPOS_ASSERT(nullptr != pcrsRequired);
	GPOS_ASSERT(2 == exprhdl.Arity());

	CColRefSet *pcrs = GPOS_NEW(m_mp) CColRefSet(m_mp);
	// include defined column
	pcrs->Include(m_aggexprid);

	// include output columns of the relational child
	pcrs->Union(exprhdl.DeriveOutputColumns(0 /*child_index*/));

	BOOL fProvidesCols = pcrs->ContainsAll(pcrsRequired);
	pcrs->Release();

	return fProvidesCols;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalTupSplit::PdsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const
{
	CDistributionSpec *pdsOuter = exprhdl.Pdpplan(0 /*child_index*/)->Pds();

	GPOS_ASSERT(nullptr != mp);

	if (CDistributionSpec::EdtHashed != pdsOuter->Edt())
	{
		pdsOuter->AddRef();
		return pdsOuter;
	}

	return GPOS_NEW(mp) CDistributionSpecRandom();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::PrsDerive
//
//	@doc:
//		Derive rewindability
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalTupSplit::PrsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const
{
	return PrsDerivePassThruOuter(mp, exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::HashValue
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CPhysicalTupSplit::HashValue() const
{
	ULONG ulHash = gpos::CombineHashes(COperator::HashValue(),
									   CUtils::UlHashColArray(m_dqaexprs));
	ulHash = gpos::CombineHashes(ulHash, gpos::HashPtr<CColRef>(m_aggexprid));

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::Matches
//
//	@doc:
//		Match operator
//
//---------------------------------------------------------------------------
BOOL
CPhysicalTupSplit::Matches(COperator *pop) const
{
	if (pop->Eopid() == Eopid())
	{
		CPhysicalTupSplit *popSplit = CPhysicalTupSplit::PopConvert(pop);

		return m_aggexprid == popSplit->PcrAggexprid() &&
			   m_dqaexprs->Equals(popSplit->Pdrgpcrdqaexprs()) &&
			   m_pdrgpcr->Equals(popSplit->Pdrgpcr());
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::EpetRewindability
//
//	@doc:
//		Return the enforcing type for rewindability property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalTupSplit::EpetRewindability(CExpressionHandle &exprhdl,
								  const CEnfdRewindability *per) const
{
	// get rewindability delivered by the split node
	CRewindabilitySpec *prs = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Prs();
	if (per->FCompatible(prs))
	{
		// required rewindability is already provided
		return CEnfdProp::EpetUnnecessary;
    }

	// always force spool to be on top of split
	return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalTupSplit::OsPrint(IOstream &os) const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}

	os << SzId() << " DQAEXPRs Columns: [";
	CUtils::OsPrintDrgPcr(os, m_dqaexprs);
	os << "], Group Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcr);
	os << "], AGGEXPRID: ";
	m_aggexprid->OsPrint(os);

	return os;
}


// EOF
