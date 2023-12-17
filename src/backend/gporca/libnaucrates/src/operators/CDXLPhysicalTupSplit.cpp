//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalTupSplit.cpp
//
//	@doc:
//		Implementation of DXL physical TupSplit operator
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLPhysicalTupSplit.h"

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLTableDescr.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSplit::CDXLPhysicalTupSplit
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLPhysicalTupSplit::CDXLPhysicalTupSplit(CMemoryPool *mp,
									 ULONG aggexprid,
									 ULongPtrArray *dqexprs,
									 ULongPtrArray *pdrgpcr)
	: CDXLPhysical(mp),
	  m_aggexprid(aggexprid),
	  m_dqaexprs(dqexprs),
	  m_pdrgpcr(pdrgpcr)
{
	GPOS_ASSERT(nullptr != dqexprs);
	GPOS_ASSERT(nullptr != pdrgpcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalTupSplit::~CDXLPhysicalTupSplit
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLPhysicalTupSplit::~CDXLPhysicalTupSplit()
{
	m_dqaexprs->Release();
	m_pdrgpcr->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalTupSplit::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalTupSplit::GetDXLOperator() const
{
	return EdxlopPhysicalTupSplit;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalTupSplit::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalTupSplit::GetOpNameStr() const
{
	return CDXLTokens::GetDXLTokenStr(EdxltokenPhysicalTupSplit);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalTupSplit::SerializeToDXL
//
//	@doc:
//		Serialize function descriptor in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalTupSplit::SerializeToDXL(CXMLSerializer *xml_serializer,
								  const CDXLNode *dxlnode) const
{
	const CWStringConst *element_name = GetOpNameStr();
	xml_serializer->OpenElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix), element_name);

	CWStringDynamic *dqaexpr_cols =
		CDXLUtils::Serialize(m_mp, m_dqaexprs);
	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(EdxltokenDQAExprs), dqaexpr_cols);
	GPOS_DELETE(dqaexpr_cols);

	CWStringDynamic *group_cols =
		CDXLUtils::Serialize(m_mp, m_pdrgpcr);
	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(EdxltokenGroupingCols), group_cols);
	GPOS_DELETE(group_cols);

	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(EdxltokenActionColId), m_aggexprid);

	dxlnode->SerializePropertiesToDXL(xml_serializer);

	// serialize project list
	(*dxlnode)[0]->SerializeToDXL(xml_serializer);

	// serialize physical child
	(*dxlnode)[1]->SerializeToDXL(xml_serializer);

	xml_serializer->CloseElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix), element_name);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalTupSplit::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLPhysicalTupSplit::AssertValid(const CDXLNode *dxlnode,
							   BOOL validate_children) const
{
	GPOS_ASSERT(2 == dxlnode->Arity());
	CDXLNode *child_dxlnode = (*dxlnode)[1];
	GPOS_ASSERT(EdxloptypePhysical ==
				child_dxlnode->GetOperator()->GetDXLOperatorType());

	if (validate_children)
	{
		child_dxlnode->GetOperator()->AssertValid(child_dxlnode,
												  validate_children);
	}
}

#endif	// GPOS_DEBUG


// EOF
