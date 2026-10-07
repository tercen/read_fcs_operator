"""Add TableStep(documentId -> file) -> DataStep(documentId on columns, gather_channels=true) to a Studio workflow."""
import os, sys, uuid
import tercen.model.impl as m
from tercen.client.factory import TercenClient

uri, token = "http://127.0.0.1:5402", os.environ["TERCEN_TOKEN"]
wf_id, file_id, step_name = sys.argv[1], sys.argv[2], sys.argv[3]
props = dict(a.split("=", 1) for a in sys.argv[4:])

client = TercenClient(uri)
client.userService.tercenClient.token = token
client.httpClient.authorization = token
wf = client.workflowService.get(wf_id)

def rect(x, y, w=200.0, h=55.0):
    r = m.Rectangle(); r.topLeft = m.Point(); r.topLeft.x = x; r.topLeft.y = y
    r.extent = m.Point(); r.extent.x = w; r.extent.y = h; return r
def gf(name, typ="string"):
    g = m.GraphicalFactor(); g.factor = m.Factor(); g.factor.name = name; g.factor.type = typ
    g.rectangle = rect(0.0, 0.0, max(len(name) * 10.0, 30.0), 30.0); return g
def ctable(factors):
    t = m.CrosstabTable(); t.cellSize = 250.0; t.offset = 0; t.nRows = 0
    t.graphicalFactors = factors; t.rectangleSelections = []; return t

# --- TableStep ---
ts = m.TableStep(); ts.id = str(uuid.uuid4()); ts.name = step_name; ts.groupId = ""; ts.description = ""
op = m.OutputPort(); op.id = str(uuid.uuid4()); op.name = "table"; op.linkType = "relation"
ts.inputs = []; ts.outputs = [op]; ts.rectangle = rect(100.0, 100.0)
ts.state = m.StepState(); ts.state.taskId = ""; ts.state.taskState = m.DoneState()
tbl = m.Table(); tbl.nRows = 1
tbl.properties = m.TableProperties(); tbl.properties.name = ""; tbl.properties.sortOrder = []; tbl.properties.ascending = False
col = m.Column(); col.id = ""; col.name = "documentId"; col.type = "string"; col.nRows = 1; col.size = 1; col.values = [file_id]
tbl.columns = [col]
rel = m.InMemoryRelation(); rel.id = str(uuid.uuid4()); rel.inMemoryTable = tbl
ts.model = m.TableStepModel(); ts.model.relation = rel; ts.model.filterSelector = ""

# --- DataStep ---
ds = m.DataStep(); ds.id = str(uuid.uuid4()); ds.name = "read_fcs (Rust, dev)"; ds.groupId = ""; ds.description = ""; ds.parentDataStepId = ""
ip = m.InputPort(); ip.id = str(uuid.uuid4()); ip.name = "data"; ip.linkType = "relation"
dop = m.OutputPort(); dop.id = str(uuid.uuid4()); dop.name = "data"; dop.linkType = "relation"
ds.inputs = [ip]; ds.outputs = [dop]; ds.rectangle = rect(100.0, 250.0)
ds.state = m.StepState(); ds.state.taskId = ""; ds.state.taskState = m.InitState()
ct = m.Crosstab(); ct.taskId = ""
ct.axis = m.XYAxisList(); ct.axis.rectangleSelections = []; ct.axis.xyAxis = []
ct.columnTable = ctable([gf("documentId")])
ct.rowTable = ctable([gf("")])
ct.filters = m.Filters(); ct.filters.removeNaN = False; ct.filters.namedFilters = []
os_ = m.OperatorSettings(); os_.namespace = "ds0"; os_.environment = []
ref = m.OperatorRef(); ref.name = "read_fcs_operator"; ref.version = "dev"; ref.operatorId = ""; ref.operatorKind = ""
ref.url = m.Url(); ref.url.uri = "https://github.com/tercen/read_fcs_operator"
ref.propertyValues = []
for k, v in props.items():
    pv = m.PropertyValue(); pv.name = k; pv.value = v; ref.propertyValues.append(pv)
ref.operatorSpec = m.OperatorSpec(); ref.operatorSpec.inputSpecs = []; ref.operatorSpec.outputSpecs = []
os_.operatorRef = ref
ct.operatorSettings = os_
ds.model = ct

link = m.Link(); link.id = str(uuid.uuid4()); link.inputId = ip.id; link.outputId = op.id

wf.steps = list(wf.steps or []) + [ts, ds]
wf.links = list(wf.links or []) + [link]
client.workflowService.update(wf)
print("TABLE_STEP", ts.id); print("DATA_STEP", ds.id)
