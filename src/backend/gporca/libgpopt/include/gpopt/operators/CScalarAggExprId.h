//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2024 VMware, Inc.
//
//	@filename:
//		CScalarAggExprId.h
//
//	@doc:
//		Scalar AggExprId operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarAggExprId_H
#define GPOPT_CScalarAggExprId_H

#include "gpos/base.h"

#include "gpopt/base/CDrvdProp.h"
#include "gpopt/operators/CScalar.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CScalarAggExprId
//
//	@doc:
//		Scalar casting operator
//
//---------------------------------------------------------------------------
class CScalarAggExprId : public CScalar
{
private:
public:
	CScalarAggExprId(const CScalarAggExprId &) = delete;

	// ctor
	CScalarAggExprId(CMemoryPool *mp) : CScalar(mp)
	{
	}

	// dtor
	~CScalarAggExprId() override = default;
	// ident accessors

	// the type of the scalar expression
	IMDId *MdidType() const override;

	EOperatorId
	Eopid() const override
	{
		return EopScalarAggExprId;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CScalarAggExprId";
	}

	// match function
	BOOL Matches(COperator *pop) const override;

	// sensitivity to order of inputs
	BOOL
	FInputOrderSensitive() const override
	{
		return false;
	}

	// return a copy of the operator with remapped columns
	COperator *
	PopCopyWithRemappedColumns(CMemoryPool *,		//mp,
							   UlongToColRefMap *,	//colref_mapping,
							   BOOL					//must_exist
							   ) override
	{
		return PopCopyDefault();
	}

	// conversion function
	static CScalarAggExprId *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopScalarAggExprId == pop->Eopid());

		return dynamic_cast<CScalarAggExprId *>(pop);
	}

};	// class CScalarAggExprId
}  // namespace gpopt

#endif	// !GPOPT_CScalarAggExprId_H

// EOF
