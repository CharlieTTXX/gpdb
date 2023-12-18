//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLScalarAggExprId.h
//
//	@doc:
//		Class for representing DXL DML action expressions
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarAggExprId_H
#define GPDXL_CDXLScalarAggExprId_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLScalar.h"

namespace gpdxl
{
//---------------------------------------------------------------------------
//	@class:
//		CDXLScalarAggExprId
//
//	@doc:
//		Class for representing DXL DML action expressions
//
//---------------------------------------------------------------------------
class CDXLScalarAggExprId : public CDXLScalar
{
private:
public:
	CDXLScalarAggExprId(const CDXLScalarAggExprId &) = delete;

	// ctor/dtor
	explicit CDXLScalarAggExprId(CMemoryPool *mp);

	~CDXLScalarAggExprId() override = default;

	// ident accessors
	Edxlopid GetDXLOperator() const override;

	const CWStringConst *GetOpNameStr() const override;

	// serialize operator in DXL format
	void SerializeToDXL(CXMLSerializer *xml_serializer,
						const CDXLNode *node) const override;

	// conversion function
	static CDXLScalarAggExprId *
	Cast(CDXLOperator *dxl_op)
	{
		GPOS_ASSERT(nullptr != dxl_op);
		GPOS_ASSERT(EdxlopScalarDMLAction == dxl_op->GetDXLOperator());

		return dynamic_cast<CDXLScalarAggExprId *>(dxl_op);
	}

	// does the operator return a boolean result
	BOOL HasBoolResult(CMDAccessor *md_accessor) const override;

#ifdef GPOS_DEBUG
	// checks whether the operator has valid structure, i.e. number and
	// types of child nodes
	void AssertValid(const CDXLNode *node,
					 BOOL validate_children) const override;
#endif	// GPOS_DEBUG
};
}  // namespace gpdxl

#endif	// !GPDXL_CDXLScalarAggExprId_H

// EOF
