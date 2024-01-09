//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2024 VMware, Inc.
//
//	@filename:
//		CXformGbAggWithMDQA2TupSplit.h
//
//	@doc:
//		Transform a GbAgg with multiple distinct qualified aggregates (MDQAs)
//		to a join tree with single DQA leaves
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformGbAggWithMDQA2TupSplit_H
#define GPOPT_CXformGbAggWithMDQA2TupSplit_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"
#include "gpopt/base/CUtils.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformGbAggWithMDQA2TupSplit
//
//	@doc:
//		Transform a GbAgg with multiple distinct qualified aggregates (MDQAs)
//		to a join tree with single DQA leaves
//
//---------------------------------------------------------------------------
class CXformGbAggWithMDQA2TupSplit : public CXformExploration
{
private:
	// hash map between expression and a column reference
	using ExprToColRefMap =
		CHashMap<CExpression, CColRef, CExpression::HashValue, CUtils::Equals,
				 CleanupRelease<CExpression>, CleanupNULL<CColRef>>;

	static CExpression *PexprTupSplitMDQAs(CMemoryPool *mp, CExpression *pexpr);

	static void ExtractDistinctCols(CMemoryPool *mp, CColumnFactory *col_factory, CMDAccessor *md_accessor,
									CExpression *pexpr, CExpressionArray *pdrgpexprChildPrEl,
									ExprToColRefMap *phmexprcr,
									CColRefArray **ppdrgpcrArgDQA);  // output: array of distinct aggs arguments

	static CColRef *PcrAggFuncArgument(CMemoryPool *mp, CMDAccessor *md_accessor,
								CColumnFactory *col_factory,
								CExpression *pexprArg,
								CExpressionArray *pdrgpexprChildPrEl);
	static CExpression *PexprTupSplitAggregations(
								CMemoryPool *mp, CExpression *pexprRelational,
								CExpressionArray *pdrgpexprPrElFirstStage,
								CExpressionArray *pdrgpexprPrElThirdStage, CColRefArray *pdrgpcrArgDQA,
								CColRefArray *pdrgpcrLastStage, CColRefArray *dqaexprs, CColRef *aggexprid);

public:
	CXformGbAggWithMDQA2TupSplit(const CXformGbAggWithMDQA2TupSplit &) = delete;

	// ctor
	explicit CXformGbAggWithMDQA2TupSplit(CMemoryPool *mp);

	// dtor
	~CXformGbAggWithMDQA2TupSplit() override = default;

	// ident accessors
	EXformId
	Exfid() const override
	{
		return ExfGbAggWithMDQA2TupSplit;
	}

	// return a string for xform name
	const CHAR *
	SzId() const override
	{
		return "CXformGbAggWithMDQA2TupSplit";
	}

	// compute xform promise for a given expression handle
	EXformPromise Exfp(CExpressionHandle &exprhdl) const override;

	// actual transform
	void Transform(CXformContext *, CXformResult *,
				   CExpression *) const override;

	// return true if xform should be applied only once
	BOOL IsApplyOnce() override;

};	// class CXformGbAggWithMDQA2TupSplit

}  // namespace gpopt

#endif	// !GPOPT_CXformGbAggWithMDQA2TupSplit_H

// EOF
