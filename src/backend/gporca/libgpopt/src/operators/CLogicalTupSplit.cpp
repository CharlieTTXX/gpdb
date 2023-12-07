//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalTupSplit.cpp
//
//	@doc:
//		Implementation of logical tup split operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalTupSplit.h"

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CKeyCollection.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTupSplit::CLogicalTupSplit
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalTupSplit::CLogicalTupSplit(CMemoryPool *mp)
	: CLogical(mp),
	  m_pdrgpcrDelete(nullptr),
	  m_pdrgpcrInsert(nullptr),
	  m_pcrCtid(nullptr),
	  m_pcrSegmentId(nullptr),
	  m_pcrAction(nullptr)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTupSplit::CLogicalTupSplit
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalTupSplit::CLogicalTupSplit(CMemoryPool *mp, CColRefArray *pdrgpcrDelete,
							 CColRefArray *pdrgpcrInsert, CColRef *pcrCtid,
							 CColRef *pcrSegmentId, CColRef *pcrAction)
	: CLogical(mp),
	  m_pdrgpcrDelete(pdrgpcrDelete),
	  m_pdrgpcrInsert(pdrgpcrInsert),
	  m_pcrCtid(pcrCtid),
	  m_pcrSegmentId(pcrSegmentId),
	  m_pcrAction(pcrAction)

{
	GPOS_ASSERT(nullptr != pdrgpcrDelete);
	GPOS_ASSERT(nullptr != pdrgpcrInsert);
	GPOS_ASSERT(pdrgpcrInsert->Size() == pdrgpcrDelete->Size());
	GPOS_ASSERT(nullptr != pcrCtid);
	GPOS_ASSERT(nullptr != pcrSegmentId);
	GPOS_ASSERT(nullptr != pcrAction);

	m_pcrsLocalUsed->Include(m_pdrgpcrDelete);
	m_pcrsLocalUsed->Include(m_pdrgpcrInsert);
	m_pcrsLocalUsed->Include(m_pcrCtid);
	m_pcrsLocalUsed->Include(m_pcrSegmentId);
	m_pcrsLocalUsed->Include(m_pcrAction);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTupSplit::~CLogicalTupSplit
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalTupSplit::~CLogicalTupSplit()
{
	CRefCount::SafeRelease(m_pdrgpcrDelete);
	CRefCount::SafeRelease(m_pdrgpcrInsert);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTupSplit::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CLogicalTupSplit::Matches(COperator *pop) const
{
	if (pop->Eopid() == Eopid())
	{
		CLogicalTupSplit *popSplit = CLogicalTupSplit::PopConvert(pop);

		return m_aggexprid == popSplit->GetExprId();
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTupSplit::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalTupSplit::HashValue() const
{
	ULONG ulHash = gpos::CombineHashes(COperator::HashValue(),
									   gpos::HashPtr<CColRef>(m_aggexprid));

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTupSplit::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalTupSplit::PopCopyWithRemappedColumns()
{
    return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTupSplit::DeriveOutputColumns
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalTupSplit::DeriveOutputColumns(CMemoryPool *mp, CExpressionHandle &exprhdl)
{
	GPOS_ASSERT(2 == exprhdl.Arity());

	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Union(exprhdl.DeriveOutputColumns(0));
	pcrs->Include(m_pcrAction);

	return pcrs;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalTupSplit::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalTupSplit::DeriveKeyCollection(CMemoryPool *,  // mp
								   CExpressionHandle &exprhdl) const
{
	return PkcDeriveKeysPassThru(exprhdl, 0 /* ulChild */);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalTupSplit::DeriveMaxCard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalTupSplit::DeriveMaxCard(CMemoryPool *,	 // mp
							 CExpressionHandle &exprhdl) const
{
	// pass on max card of first child
	return exprhdl.DeriveMaxCard(0);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTupSplit::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalTupSplit::PxfsCandidates(CMemoryPool *mp) const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);
	(void) xform_set->ExchangeSet(CXform::ExfImplementSplit);
	return xform_set;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTupSplit::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalTupSplit::PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl,
							   IStatisticsArray *	// not used
) const
{
	// split returns double the number of tuples coming from its child
	IStatistics *stats = exprhdl.Pstats(0);

	return stats->ScaleStats(mp, CDouble(2.0) /*factor*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTupSplit::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalTupSplit::OsPrint(IOstream &os) const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}

	m_aggexprid->OsPrint(os);

	return os;
}

// EOF
