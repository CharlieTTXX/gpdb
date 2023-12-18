//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CScalarAggExprId.cpp
//
//	@doc:
//		Implementation of scalar AggExprId operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CScalarAggExprId.h"

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "naucrates/md/IMDTypeInt4.h"

using namespace gpopt;
using namespace gpmd;


//---------------------------------------------------------------------------
//	@function:
//		CScalarDMLAction::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarAggExprId::Matches(COperator *pop) const
{
	return pop->Eopid() == Eopid();
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarDMLAction::MdidType
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
IMDId *
CScalarAggExprId::MdidType() const
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	return md_accessor->PtMDType<IMDTypeInt4>()->MDId();
}
// EOF
