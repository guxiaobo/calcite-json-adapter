package org.apache.calcite.adapter.json;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;


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
	    Integer[] fields = getProjectFields(project.getProjects());
	    if(fields == null || fields.length == 0)
	    	return ;
	    for(int i = 0; i < fields.length; i++) {
	    	if(fields[i] == null)
	    		return;
	    }

	    call.transformTo(
	      new JsonTableScan(scan.getCluster(), scan.getTable(), fields)
	    );
	  }
	  /*
	  private String[] getProjectFields(LogicalProject project) {
		  List<Pair<RexNode, String>> namedProjs = 
		  project.getNamedProjects();
		  String[] names = new String[namedProjs.size()];
		  for(int i = 0; i < namedProjs.size(); i++) {
			  names[i] = namedProjs.get(i).getValue();
		  
		  return names;
	  }*/
	  
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
	  }
}
