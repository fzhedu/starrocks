package com.starrocks.analysis;
import com.google.common.base.Preconditions;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.thrift.TExprNode;

public class LambdaArgument extends Expr {
    public String getName() {
        return name;
    }

    String name;

    @Override
    public Type getType() {
        return type;
    }

    Type type = Type.INT;
    public LambdaArgument(String name_) {
        name = name_;
    }

    public LambdaArgument(LambdaArgument rhs) {
        super(rhs);
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        Preconditions.checkState(false, "unreachable");
    }

    @Override
    protected String toSqlImpl() {
        return String.format("%s", name);
    }

    @Override
    protected void toThrift(TExprNode msg) {
        throw new StarRocksPlannerException("not support", ErrorType.INTERNAL_ERROR);
    }

    @Override
    public Expr clone() {
        return new LambdaArgument(this);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitLambdaArgument(this, context);
    }
}
