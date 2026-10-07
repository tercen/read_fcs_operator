import os, sys, json
import tercen.model.impl as m
from tercen.client.factory import TercenClient
token = os.environ["TERCEN_TOKEN"]; wf_id, step_id, task_id = sys.argv[1:4]
client = TercenClient("http://127.0.0.1:5402"); client.userService.tercenClient.token = token; client.httpClient.authorization = token
wf = client.workflowService.get(wf_id)
ds = next(s for s in wf.steps if s.id == step_id)
axis_json = json.loads('{"kind":"XYAxisList","xyAxis":[{"kind":"XYAxis","chart":{"kind":"ChartPoint","name":"","pointSize":4,"properties":{"kind":"Properties","properties":[],"propertyValues":[]}},"xAxis":{"kind":"Axis","axisExtent":{"x":80.0,"y":30.0,"kind":"Point"},"axisSettings":{"kind":"AxisSettings","meta":[]},"graphicalFactor":{"kind":"GraphicalFactor","factor":{"kind":"Factor","name":"","type":"string"},"rectangle":{"kind":"Rectangle","extent":{"x":0.0,"y":0.0,"kind":"Point"},"topLeft":{"x":0.0,"y":0.0,"kind":"Point"}}}},"yAxis":{"kind":"Axis","axisExtent":{"x":80.0,"y":30.0,"kind":"Point"},"axisSettings":{"kind":"AxisSettings","meta":[]},"graphicalFactor":{"kind":"GraphicalFactor","factor":{"kind":"Factor","name":"","type":"string"},"rectangle":{"kind":"Rectangle","extent":{"x":0.0,"y":0.0,"kind":"Point"},"topLeft":{"x":0.0,"y":0.0,"kind":"Point"}}}},"colors":{"kind":"Colors","factors":[],"palette":{"kind":"CategoryPalette","backcolor":0,"colorList":{"kind":"ColorList","name":""},"properties":[],"stringColorElements":[]}},"errors":{"kind":"Errors","factors":[]},"labels":{"kind":"Labels","factors":[]},"taskId":"","preprocessors":[]}],"rectangleSelections":[]}')
axis_json["xyAxis"][0]["taskId"] = task_id
ds.model.axis = m.XYAxisList(axis_json)
client.workflowService.update(wf)
print("AXIS_SET")
