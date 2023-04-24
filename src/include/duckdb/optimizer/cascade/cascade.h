//---------------------------------------------------------------------------
//	@filename:
//		cascade.h
//
//	@doc:
//		Entry point to Cascade optimizer
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef CASCADE_H
#define CASCADE_H

class Cascade {
public:
        explicit Cascade(ClientContext &context)
            : context(context), cardinality_estimator(context);
        unique_ptr<PhysicalOperator> Optimize(unique_ptr<LogicalOperator> plan);
private:
        ClientContext &context;
        CardinalityEstimator cardinality_estimator;
}

#endif
