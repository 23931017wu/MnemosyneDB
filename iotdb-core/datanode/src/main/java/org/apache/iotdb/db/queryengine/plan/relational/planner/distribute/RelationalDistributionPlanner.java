/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.queryengine.plan.relational.planner.distribute;

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.SubPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;

import java.util.List;

public class RelationalDistributionPlanner {
  private final Analysis analysis;
  private final LogicalQueryPlan logicalQueryPlan;
  private final MPPQueryContext context;

  public RelationalDistributionPlanner(
      Analysis analysis, LogicalQueryPlan logicalQueryPlan, MPPQueryContext context) {
    this.analysis = analysis;
    this.logicalQueryPlan = logicalQueryPlan;
    this.context = context;
  }

  public DistributedQueryPlan plan() {
    List<PlanNode> distributedPlanNodeResult =
        new ExchangeNodeGenerator()
            .visitPlan(
                logicalQueryPlan.getRootNode(),
                new ExchangeNodeGenerator.DistributionPlanContext());

    if (distributedPlanNodeResult.size() != 1) {
      throw new IllegalStateException("root node must return only one");
    }

    SubPlan subPlan = new SubPlanGenerator().splitToSubPlan(logicalQueryPlan);
    subPlan.getPlanFragment().setRoot(true);

    List<FragmentInstance> fragmentInstances =
        new FragmentInstanceGenerator(subPlan, analysis, context).plan();

    return new DistributedQueryPlan(
        logicalQueryPlan.getContext(), subPlan, subPlan.getPlanFragmentList(), fragmentInstances);
  }
}
