"""Create + run a CubeQueryTask for a hand-built DataStep and set model.taskId (what the UI does)."""
import os, sys, time
import tercen.model.impl as m
from tercen.client.factory import TercenClient
uri, token = "http://127.0.0.1:5402", os.environ["TERCEN_TOKEN"]
wf_id, step_id = sys.argv[1], sys.argv[2]
client = TercenClient(uri); client.userService.tercenClient.token = token; client.httpClient.authorization = token
wf = client.workflowService.get(wf_id)
ds = next(s for s in wf.steps if s.id == step_id)
link = next(l for l in wf.links if l.inputId == ds.inputs[0].id)
ts = next(s for s in wf.steps if any(o.id == link.outputId for o in s.outputs))
q = m.CubeQuery()
q.relation = ts.model.relation
q.colColumns = [gf.factor for gf in ds.model.columnTable.graphicalFactors if gf.factor.name != ""]
q.rowColumns = [gf.factor for gf in ds.model.rowTable.graphicalFactors if gf.factor.name != ""]
aq = m.CubeAxisQuery(); aq.chartType = "point"; aq.pointSize = 4
aq.xAxis = m.Factor(); aq.xAxis.name = ""; aq.xAxis.type = "string"
aq.yAxis = m.Factor(); aq.yAxis.name = ""; aq.yAxis.type = "string"
aq.colors = []; aq.errors = []; aq.labels = []; aq.preprocessors = []
aq.xAxisSettings = m.AxisSettings(); aq.xAxisSettings.meta = []
aq.yAxisSettings = m.AxisSettings(); aq.yAxisSettings.meta = []
q.axisQueries = [aq]
q.filters = ds.model.filters
q.operatorSettings = ds.model.operatorSettings
task = m.CubeQueryTask(); task.state = m.InitState(); task.owner = wf.acl.owner; task.projectId = wf.projectId; task.query = q
task = client.taskService.create(task)
client.taskService.runTask(task.id)
task = client.taskService.waitDone(task.id)
print("TASK", task.id, task.state.kind if hasattr(task.state,'kind') else type(task.state).__name__, getattr(task.state, "reason", ""))
if type(task.state).__name__ == "DoneState":
    ds.model.taskId = task.id
    client.workflowService.update(wf)
    print("STEP_TASK_SET", task.query.columnHash)
