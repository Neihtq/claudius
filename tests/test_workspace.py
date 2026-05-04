from claudius.session.workspace import Workspace

def test_setup_creates_directory(tmp_path):
    workspace = Workspace(base_path=tmp_path, session_id="sess-test")
    workspace.setup()
    assert (tmp_path / "sess-test").is_dir()

def test_path_property(tmp_path):
    workspace = Workspace(base_path=tmp_path, session_id="sess-test")
    assert workspace.path == tmp_path / "sess-test"

def test_setup_idempotent(tmp_path):
    workspace = Workspace(base_path=tmp_path, session_id="sess-test")
    workspace.setup()
    workspace.setup()

def test_archive_noop(tmp_path):
    workspace = Workspace(base_path=tmp_path, session_id="sess-test")
    workspace.setup()
    workspace.archive()

def test_restore_noop(tmp_path):
    workspace = Workspace(base_path=tmp_path, session_id="sess-test")
    workspace.setup()
    workspace.restore()
