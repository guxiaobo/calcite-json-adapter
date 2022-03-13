package org.apache.calcite.adapter.json;

import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
/*
@Value.Enclosing
public class JsonProjectTableScanRule
    extends RelRule<JsonProjectTableScanRule.Config> {

  
  protected JsonProjectTableScanRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final LogicalProject project = call.rel(0);
    final JsonTableScan scan = call.rel(1);
    int[] fields = getProjectFields(project.getProjects());
    if (fields == null) {
      // Project contains expressions more complex than just field references.
      return;
    }
    call.transformTo(
        new JsonTableScan(
            scan.getCluster(),
            scan.getTable(),
            fields));
  }

  private static int[] getProjectFields(List<RexNode> exps) {
    final int[] fields = new int[exps.size()];
    for (int i = 0; i < exps.size(); i++) {
      final RexNode exp = exps.get(i);
      if (exp instanceof RexInputRef) {
        fields[i] = ((RexInputRef) exp).getIndex();
      } else {
        return null; // not a simple projection
      }
    }
    return fields;
  }


  @Value.Immutable(singleton = false)
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableCsvProjectTableScanRule.Config.builder()
        .withOperandSupplier(b0 ->
            b0.operand(LogicalProject.class).oneInput(b1 ->
                b1.operand(JsonTableScan.class).noInputs()))
        .build();

    @Override default JsonProjectTableScanRule toRule() {
      return new JsonProjectTableScanRule(this);
    }
  }
}
*/

public class JsonProjectTableScanRule  extends RelOptRule {
	static final JsonProjectTableScanRule INSTANCE = new JsonProjectTableScanRule();

	  @SuppressWarnings("deprecation")
	public JsonProjectTableScanRule() {
	    super(RelOptRule.operand(
	      LogicalProject.class,
	      RelOptRule.operand(JsonTableScan.class, RelOptRule.none())
	    ), "JsonProjectTableScanRule");
	  }

	  @Override
	  public void onMatch(RelOptRuleCall call) {
	    LogicalProject project = call.rel(0);
	    JsonTableScan scan = call.rel(1);
	    //Integer[] fields = getProjectFields(project.getProjects());

	    call.transformTo(
	      new JsonTableScan(scan.getCluster(), scan.getTable(), getProjectFields(project))
	    );
	  }
	  private String[] getProjectFields(LogicalProject project) {
		  List<Pair<RexNode, String>> namedProjs = 
		  project.getNamedProjects();
		  String[] names = new String[namedProjs.size()];
		  for(int i = 0; i < namedProjs.size(); i++) {
			  names[i] = namedProjs.get(i).getValue();
		  }
		  return names;
	  }
	  
	  /*
	  private Integer[] getProjectFields(List<RexNode> exps) {
	    List<Integer> indexes = exps.stream().map(rexNode -> {
	      if (rexNode instanceof RexInputRef) {
	        return ((RexInputRef)rexNode).getIndex();
	      }
	      return null;
	    }).collect(Collectors.toList());

	    Integer[] ret = new Integer[indexes.size()];
	    indexes.toArray(ret);
	    return ret;
	  }*/

}
