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
	  m_aggexprid(nullptr),
	  m_dqaexprs(nullptr)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTupSplit::CLogicalTupSplit
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalTupSplit::CLogicalTupSplit(CMemoryPool *mp,
								   CColRef *aggexprid,
								   CColRefArray *dqaexprs)
	: CLogical(mp),
	  m_aggexprid(aggexprid),
	  m_dqaexprs(dqaexprs)
{
	GPOS_ASSERT(nullptr != aggexprid);
	GPOS_ASSERT(nullptr != dqaexprs);

	m_pcrsLocalUsed->Include(m_aggexprid);
	m_pcrsLocalUsed->Include(m_dqaexprs);
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
	CRefCount::SafeRelease(m_dqaexprs);
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
CLogicalTupSplit::PopCopyWithRemappedColumns(CMemoryPool *mp,
										  UlongToColRefMap *colref_mapping,
										  BOOL must_exist)
{
	CColRefArray *dqaexprs =
		CUtils::PdrgpcrRemap(mp, m_dqaexprs, colref_mapping, must_exist);
	CColRef *aggexprid = CUtils::PcrRemap(m_aggexprid, colref_mapping, must_exist);

	return GPOS_NEW(mp) CLogicalTupSplit(mp, aggexprid, dqaexprs);
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
	pcrs->Include(m_aggexprid);
	pcrs->Include(m_dqaexprs);

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

	//	(void) xform_set->ExchangeSet(CXform::ExfImplementSplit);
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

	os << SzId() << " DQAEXPRs Columns: [";
	CUtils::OsPrintDrgPcr(os, m_dqaexprs);
	os << "], AGGEXPRID: ";
	m_aggexprid->OsPrint(os);

	return os;
}

// EOF
