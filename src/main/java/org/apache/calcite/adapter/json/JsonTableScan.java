package org.apache.calcite.adapter.json;

import java.util.Arrays;
import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;



public class JsonTableScan 
	extends TableScan 
	implements EnumerableRel {

	private Integer[] fields;

	@SuppressWarnings("deprecation")
	public JsonTableScan(RelOptCluster cluster, RelOptTable table, Integer[] fields) {
	    super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), table);
	    this.fields = fields;
	  }

	 @Override
	  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
	    return new JsonTableScan(getCluster(), this.table, this.fields);
	  }

	  @Override
	  public RelDataType deriveRowType() {  
		  List<RelDataTypeField> fieldList = getTable().getRowType().getFieldList();
	    RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
	    Arrays.stream(fields).forEach(field -> builder.add(fieldList.get(field)));
	    return builder.build();
	  }

	  @Override
	  public void register(RelOptPlanner planner) {
	    planner.addRule(JsonProjectTableScanRule.INSTANCE);
	  }

	  @Override
	  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
	    PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), pref.preferArray());
	    return implementor.result(physType, Blocks.toBlock(
	      Expressions.call(
	        this.table.getExpression(JsonTable.class),
	        "project",
	        implementor.getRootExpression(),
	        Expressions.constant(this.fields)
	      )
	    ));
	  }

}
