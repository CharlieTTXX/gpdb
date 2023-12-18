//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalTupSplit.h
//
//	@doc:
//		Class for representing physical tupsplit operator
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLPhysicalTupSplit_H
#define GPDXL_CDXLPhysicalTupSplit_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"

#include "naucrates/dxl/operators/CDXLPhysical.h"

namespace gpdxl
{
// fwd decl
class CDXLTableDescr;

//---------------------------------------------------------------------------
//	@class:
//		CDXLPhysicalTupSplit
//
//	@doc:
//		Class for representing physical tupsplit operators
//
//---------------------------------------------------------------------------
class CDXLPhysicalTupSplit : public CDXLPhysical
{
private:
	ULONG m_aggexprid;

	ULongPtrArray *m_dqaexprs;

	ULongPtrArray *m_pdrgpcr;

public:
	CDXLPhysicalTupSplit(const CDXLPhysicalTupSplit &) = delete;

	// ctor
	CDXLPhysicalTupSplit(CMemoryPool *mp, ULONG aggexprid, ULongPtrArray *dqaexprs,
			            ULongPtrArray *pdrgpcr);

	// dtor
	~CDXLPhysicalTupSplit() override;

	// operator type
	Edxlopid GetDXLOperator() const override;

	// operator name
	const CWStringConst *GetOpNameStr() const override;

	// dqaexprs column ids
	ULongPtrArray *
	GetDQAExprArray() const
	{
		return m_dqaexprs;
	}

	// group by column ids
	ULongPtrArray *
	GetGroupArray() const
	{
		return m_pdrgpcr;
	}

	// action column id
	ULONG
	AggExprId() const
	{
		return m_aggexprid;
	}

#ifdef GPOS_DEBUG
	// checks whether the operator has valid structure, i.e. number and
	// types of child nodes
	void AssertValid(const CDXLNode *dxlnode,
					 BOOL validate_children) const override;
#endif	// GPOS_DEBUG

	// serialize operator in DXL format
	void SerializeToDXL(CXMLSerializer *xml_serializer,
						const CDXLNode *dxlnode) const override;

	// conversion function
	static CDXLPhysicalTupSplit *
	Cast(CDXLOperator *dxl_op)
	{
		GPOS_ASSERT(nullptr != dxl_op);
		GPOS_ASSERT(EdxlopPhysicalTupSplit == dxl_op->GetDXLOperator());

		return dynamic_cast<CDXLPhysicalTupSplit *>(dxl_op);
	}
};
}  // namespace gpdxl

#endif	// !GPDXL_CDXLPhysicalTupSplit_H

// EOF
