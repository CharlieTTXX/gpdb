//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2024 VMware, Inc.
//
//	@filename:
//		CXformImplementTupSplit.h
//
//	@doc:
//		Transform Logical TupSplit to Physical TupSplit
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementTupSplit_H
#define GPOPT_CXformImplementTupSplit_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformImplementTupSplit
//
//	@doc:
//		Transform Logical TupSplit to Physical TupSplit
//
//---------------------------------------------------------------------------
class CXformImplementTupSplit : public CXformImplementation
{
private:
public:
	CXformImplementTupSplit(const CXformImplementTupSplit &) = delete;

	// ctor
	explicit CXformImplementTupSplit(CMemoryPool *mp);

	// dtor
	~CXformImplementTupSplit() override = default;

	// ident accessors
	EXformId
	Exfid() const override
	{
		return ExfImplementTupSplit;
	}

	// return a string for xform name
	const CHAR *
	SzId() const override
	{
		return "CXformImplementTupSplit";
	}

	// compute xform promise for a given expression handle
	EXformPromise Exfp(CExpressionHandle &exprhdl) const override;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const override;

};	// class CXformImplementTupSplit
}  // namespace gpopt

#endif	// !GPOPT_CXformImplementTupSplit_H

// EOF
