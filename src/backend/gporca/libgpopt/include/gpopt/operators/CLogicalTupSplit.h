//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalTupSplit.h
//
//	@doc:
//		Logical tuple split operator Multi-DQA
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalTupSplit_H
#define GPOPT_CLogicalTupSplit_H

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogical.h"

namespace gpopt
{
// fwd declarations
class CTableDescriptor;

//---------------------------------------------------------------------------
//	@class:
//		CLogicalTupSplit
//
//	@doc:
//		Logical tuple split operator
//
//---------------------------------------------------------------------------
class CLogicalTupSplit : public CLogical
{
private:
	CColRef *m_aggexprid;

	CColRefArray *m_dqaexprs;

	// array of grouping columns
	CColRefArray *m_pdrgpcr;

public:
	CLogicalTupSplit(const CLogicalTupSplit &) = delete;

	// ctor
	explicit CLogicalTupSplit(CMemoryPool *mp);

	// ctor
	CLogicalTupSplit(CMemoryPool *mp, CColRef *aggexprid, CColRefArray *dqapexrs, CColRefArray *pdrgpcr);

	// dtor
	~CLogicalTupSplit() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopLogicalTupSplit;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CLogicalTupSplit";
	}

	// deletion columns
	CColRefArray *
	GetDQAExpr() const
	{
		return m_dqaexprs;
	}

	// Group columns
	CColRefArray *
	GetGroupby() const
	{
		return m_pdrgpcr;
	}

	// ctid column
	CColRef *
	GetExprId() const
	{
		return m_aggexprid;
	}

	// operator specific hash function
	ULONG HashValue() const override;

	// match function
	BOOL Matches(COperator *pop) const override;

	// sensitivity to order of inputs
	BOOL
	FInputOrderSensitive() const override
	{
		return true;
	}

	// return a copy of the operator with remapped columns
	COperator *PopCopyWithRemappedColumns(CMemoryPool *mp,
										  UlongToColRefMap *colref_mapping,
										  BOOL must_exist) override;

	//-------------------------------------------------------------------------------------
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------

	// derive output columns
	CColRefSet *DeriveOutputColumns(CMemoryPool *mp,
									CExpressionHandle &exprhdl) override;


	// derive constraint property
	CPropConstraint *
	DerivePropertyConstraint(CMemoryPool *,	 // mp
							 CExpressionHandle &exprhdl) const override
	{
		return CLogical::PpcDeriveConstraintPassThru(exprhdl, 0 /*ulChild*/);
	}

	// derive max card
	CMaxCard DeriveMaxCard(CMemoryPool *mp,
						   CExpressionHandle &exprhdl) const override;

	// derive partition consumer info
	CPartInfo *
	DerivePartitionInfo(CMemoryPool *,	// mp,
						CExpressionHandle &exprhdl) const override
	{
		return PpartinfoPassThruOuter(exprhdl);
	}

	// compute required stats columns of the n-th child
	CColRefSet *
	PcrsStat(CMemoryPool *mp, CExpressionHandle &exprhdl, CColRefSet *pcrsInput,
			 ULONG child_index) const override
	{
		return PcrsReqdChildStats(mp, exprhdl, pcrsInput,
								  exprhdl.DeriveUsedColumns(1), child_index);
	}

	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	CXformSet *PxfsCandidates(CMemoryPool *mp) const override;

	// derive key collections
	CKeyCollection *DeriveKeyCollection(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

	// derive statistics
	IStatistics *PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl,
							  IStatisticsArray *stats_ctxt) const override;

	// stat promise
	EStatPromise
	Esp(CExpressionHandle &) const override
	{
		return CLogical::EspHigh;
	}

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// conversion function
	static CLogicalTupSplit *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopLogicalTupSplit == pop->Eopid());

		return dynamic_cast<CLogicalTupSplit *>(pop);
	}

	// debug print
	IOstream &OsPrint(IOstream &) const override;

};	// class CLogicalTupSplit
}  // namespace gpopt

#endif	// !GPOPT_CLogicalTupSplit_H

// EOF
