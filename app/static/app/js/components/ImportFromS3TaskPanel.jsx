import "../css/ImportTaskPanel.scss";
import React from "react";
import PropTypes from "prop-types";
import Dropzone from "../vendor/dropzone";
import csrf from "../django/csrf";
import ErrorMessage from "./ErrorMessage";
import UploadProgressBar from "./UploadProgressBar";
import { _, interpolate } from "../classes/gettext";
import "../css/NewTaskPanel.scss";
import EditTaskForm from "./EditTaskForm";
import Storage from "../classes/Storage";
import ResizeModes from "../classes/ResizeModes";
import update from "immutability-helper";
import PluginsAPI from "../classes/plugins/API";

class ImportFromS3TaskPanel extends React.Component {
  static defaultProps = {
    filesCount: 0,
    showResize: false,
  };

  static propTypes = {
    onSave: PropTypes.func.isRequired,
    onCancel: PropTypes.func,
    filesCount: PropTypes.number,
    showResize: PropTypes.bool,
    getFiles: PropTypes.func,
    suggestedTaskName: PropTypes.oneOfType([PropTypes.string, PropTypes.func]),

    onImported: PropTypes.func.isRequired,
    onCancel: PropTypes.func,
    projectId: PropTypes.number.isRequired,
  };

  constructor(props) {
    super(props);

    this.state = {
      editTaskFormLoaded: false,
      resizeMode:
        Storage.getItem("resize_mode") === null
          ? ResizeModes.YES
          : ResizeModes.fromString(Storage.getItem("resize_mode")),
      resizeSize: parseInt(Storage.getItem("resize_size")) || 2048,
      items: [], // Coming from plugins,
      taskInfo: {},
      inReview: false,
      loading: false,
      showMapPreview: false,

      error: "",
      typeS3Url: false,
      importingFromS3Url: false,
      progress: 0,
      bytesSent: 0,
      importS3Url: "",
      importS3Key: "",
      importS3Secret: "",
      importS3Bucket: "",

      taskId: null,
    };

    this.save = this.save.bind(this);
    this.handleFormTaskLoaded = this.handleFormTaskLoaded.bind(this);
    this.getTaskInfo = this.getTaskInfo.bind(this);
    this.setResizeMode = this.setResizeMode.bind(this);
    this.handleResizeSizeChange = this.handleResizeSizeChange.bind(this);
    this.handleFormChanged = this.handleFormChanged.bind(this);
  }

  defaultTaskName = () => {
    return `Task of ${new Date().toISOString()}`;
  };

  componentDidMount() {
    PluginsAPI.Dashboard.triggerAddNewTaskPanelItem({}, (item) => {
      if (!item) return;

      this.setState(
        update(this.state, {
          items: { $push: [item] },
        })
      );
    });

    Dropzone.autoDiscover = false;

    if (this.dropzone) {
      this.dz = new Dropzone(this.dropzone, {
        paramName: "file",
        url: `/api/projects/${this.props.projectId}/tasks/import`,
        parallelUploads: 1,
        maxFilesize: 2147483647,
        uploadMultiple: false,
        acceptedFiles:
          "application/zip,application/octet-stream,application/x-zip-compressed,multipart/x-zip",
        autoProcessQueue: true,
        createImageThumbnails: false,
        previewTemplate: '<div style="display:none"></div>',
        clickable: this.uploadButton,
        chunkSize: 2147483647,
        timeout: 2147483647,
        chunking: true,
        chunkSize: 16000000, // 16MB
        headers: {
          [csrf.header]: csrf.token,
        },
      });

      this.dz
        .on("error", (file) => {
          if (this.state.uploading)
            this.setState({
              error: _(
                "Cannot upload file. Check your internet connection and try again."
              ),
            });
        })
        .on("sending", () => {
          this.setState({
            typeS3Url: false,
            typeUrl: false,
            uploading: true,
            totalCount: 1,
          });
        })
        .on("reset", () => {
          this.setState({
            uploading: false,
            progress: 0,
            totalBytes: 0,
            totalBytesSent: 0,
          });
        })
        .on("uploadprogress", (file, progress, bytesSent) => {
          if (progress == 100) return; // Workaround for chunked upload progress bar jumping around
          this.setState({
            progress,
            totalBytes: file.size,
            totalBytesSent: bytesSent,
          });
        })
        .on("sending", (file, xhr, formData) => {
          // Safari does not have support for has on FormData
          // as of December 2017
          if (!formData.has || !formData.has("name"))
            formData.append("name", this.defaultTaskName());
        })
        .on("complete", (file) => {
          if (file.status === "success") {
            this.setState({ uploading: false });
            try {
              let response = JSON.parse(file.xhr.response);
              if (!response.id)
                throw new Error(
                  `Expected id field, but none given (${response})`
                );
              this.props.onImported();
            } catch (e) {
              this.setState({
                error: interpolate(
                  _("Invalid response from server: %(error)s"),
                  { error: e.message }
                ),
              });
            }
          } else if (this.state.uploading) {
            this.setState({
              uploading: false,
              error: _(
                "An error occured while uploading the file. Please try again."
              ),
            });
          }
        });
    }
  }

  componentDidUpdate(prevProps, prevState) {
    if (this.props.filesCount !== prevProps.filesCount && this.mapPreview) {
      this.mapPreview.loadNewFiles();
    }
  }

  cancel = (e) => {
    this.cancelUpload();

    if (this.state.inReview) {
      this.setState({ inReview: false });
    } else {
      if (this.props.onCancel) {
        if (window.confirm(_("Are you sure you want to cancel?"))) {
          this.props.onCancel();
        }
      }
    }
  };

  cancelUpload = (e) => {
    this.setState({ uploading: false });
    setTimeout(() => {
      this.dz.removeAllFiles(true);
    }, 0);
  };

  handleImportFromS3 = () => {
    this.setState({ typeS3Url: !this.state.typeS3Url, typeUrl: false });
  };

  handleCancelImportFromS3URL = () => {
    this.setState({ typeS3Url: false });
  };

  handleChangeImportS3Url = (e) => {
    this.setState({ importS3Url: e.target.value });
  };

  handleChangeImportS3Key = (e) => {
    this.setState({ importS3Key: e.target.value });
  };

  handleChangeImportS3Secret = (e) => {
    this.setState({ importS3Secret: e.target.value });
  };

  handleChangeImportS3Bucket = (e) => {
    this.setState({ importS3Bucket: e.target.value });
  };

  handleConfirmImportS3Url = () => {
    this.setState({ importingFromS3Url: true, importingFromUrl: false });

    `/api/projects/${this.state.data.id}/tasks/${task.id}/upload/`;
    $.post(`/api/projects/${this.props.projectId}/tasks/s3-import`, {
      url: this.state.importS3Url,
      accessKey: this.state.importS3Key,
      secret: this.state.importS3Secret,
      bucket: this.state.importS3Bucket,
      name: this.defaultTaskName(),
    })
      .done((json) => {
        this.setState({ importingFromS3Url: false });

        // if (json.id) {
        // this.props.onImported();
        // } else {
        //   this.setState({
        //     error:
        //       json.error ||
        //       interpolate(_("Invalid JSON response: %(error)s"), {
        //         error: JSON.stringify(json),
        //       }),
        //   });
        // }
      })
      .fail(() => {
        this.setState({
          importingFromS3Url: false,
          error: _(
            "Cannot import from this S3 URL. Check your internet connection."
          ),
        });
      });
  };

  setRef = (prop) => {
    return (domNode) => {
      if (domNode != null) this[prop] = domNode;
    };
  };

  save = (e) => {
    if (!this.state.inReview) {
      this.setState({ inReview: true });
    } else {
      this.setState({ inReview: false, loading: true });
      e.preventDefault();
      this.taskForm.saveLastPresetToStorage();
      Storage.setItem("resize_size", this.state.resizeSize);
      Storage.setItem("resize_mode", this.state.resizeMode);

      const taskInfo = this.getTaskInfo();
      if (taskInfo.selectedNode.key != "auto") {
        Storage.setItem("last_processing_node", taskInfo.selectedNode.id);
      } else {
        Storage.setItem("last_processing_node", "");
      }

      const taskEvents = {
        onSaveSuccess: (task) => {
          this.setState({ taskId: task.id });
          this.sendStartDownloadS3();
        },
        onSaveError: () => {
          console.error("nao salvou");
        },
      };

      if (this.props.onSave)
        this.props.onSave({
          taskInfo,
          taskEvents,
        });
    }
  };

  getTaskInfo() {
    return Object.assign(this.taskForm.getTaskInfo(), {
      resizeSize: this.state.resizeSize,
      resizeMode: this.state.resizeMode,
    });
  }

  setResizeMode(v) {
    return (e) => {
      this.setState({ resizeMode: v });

      setTimeout(() => {
        this.handleFormChanged();
      }, 0);
    };
  }

  handleResizeSizeChange(e) {
    // Remove all non-digit characters
    let n = parseInt(e.target.value.replace(/[^\d]*/g, ""));
    if (isNaN(n)) n = "";
    this.setState({ resizeSize: n });

    setTimeout(() => {
      this.handleFormChanged();
    }, 0);
  }

  handleFormTaskLoaded() {
    this.setState({ editTaskFormLoaded: true });
  }

  handleFormChanged() {
    this.setState({ taskInfo: this.getTaskInfo() });
  }

  handleSuggestedTaskName = () => {
    return this.props.suggestedTaskName(() => {
      // Has GPS
      this.setState({ showMapPreview: true });
    });
  };

  getCropPolygon = () => {
    if (!this.mapPreview) return null;
    return this.mapPreview.getCropPolygon();
  };

  handlePolygonChange = () => {
    if (this.taskForm) this.taskForm.forceUpdate();
  };

  sendStartDownloadS3() {
    $.post(
      `/api/projects/${this.props.projectId}/tasks/${this.state.taskId}/start-download-from-s3/`,
      {
        url: this.state.importS3Url,
        accessKey: this.state.importS3Key,
        secret: this.state.importS3Secret,
        images: this.state.importS3Bucket,
      }
    )
      .done(() => {
        this.setState({ loading: false, importingFromS3Url: false });

        this.props.onImported();
        // } else {
        //   this.setState({
        //     error:
        //       json.error ||
        //       interpolate(_("Invalid JSON response: %(error)s"), {
        //         error: JSON.stringify(json),
        //       }),
        //   });
        // }
      })
      .fail(() => {
        this.setState({
          importingFromS3Url: false,
          error: _(
            "Cannot import from this S3 URL. Check your internet connection."
          ),
        });
      });
  }

  render() {
    let filesCountOk = true;
    if (this.taskForm && !this.taskForm.checkFilesCount(this.props.filesCount))
      filesCountOk = false;

    return (
      <div className="new-task-panel theme-background-highlight">
        <div className="form-horizontal">
          <div className={this.state.inReview ? "disabled" : ""}>
            <p>
              {interpolate(
                _(
                  "%(count)s files selected. Please check these additional options:"
                ),
                { count: this.props.filesCount }
              )}
            </p>

            {!filesCountOk ? (
              <div className="alert alert-warning">
                {interpolate(
                  _(
                    "Number of files selected exceeds the maximum of %(count)s allowed on this processing node."
                  ),
                  { count: this.taskForm.selectedNodeMaxImages() }
                )}
                <button
                  onClick={this.props.onCancel}
                  type="button"
                  className="btn btn-xs btn-primary redo"
                >
                  <span>
                    <i className="glyphicon glyphicon-remove-circle"></i>{" "}
                    {_("Cancel")}
                  </span>
                </button>
              </div>
            ) : (
              ""
            )}

            {this.state.showMapPreview ? (
              <MapPreview
                getFiles={this.props.getFiles}
                onPolygonChange={this.handlePolygonChange}
                ref={(domNode) => {
                  this.mapPreview = domNode;
                }}
              />
            ) : (
              ""
            )}

            <EditTaskForm
              selectedNode={Storage.getItem("last_processing_node") || "auto"}
              onFormLoaded={this.handleFormTaskLoaded}
              onFormChanged={this.handleFormChanged}
              inReview={this.state.inReview}
              suggestedTaskName={this.handleSuggestedTaskName}
              getCropPolygon={this.getCropPolygon}
              ref={(domNode) => {
                if (domNode) this.taskForm = domNode;
              }}
            />

            {this.state.editTaskFormLoaded && this.props.showResize ? (
              <div>
                <div className="form-group">
                  <label className="col-sm-2 control-label">
                    {_("Resize Images")}
                  </label>
                  <div className="col-sm-10">
                    <div className="btn-group">
                      <button
                        type="button"
                        className="btn btn-default dropdown-toggle"
                        data-toggle="dropdown"
                      >
                        {ResizeModes.toHuman(this.state.resizeMode)}{" "}
                        <span className="caret"></span>
                      </button>
                      <ul className="dropdown-menu">
                        {ResizeModes.all().map((mode) => (
                          <li key={mode}>
                            <a
                              href="javascript:void(0);"
                              onClick={this.setResizeMode(mode)}
                            >
                              <i
                                style={{
                                  opacity:
                                    this.state.resizeMode === mode ? 1 : 0,
                                }}
                                className="fa fa-check"
                              ></i>{" "}
                              {ResizeModes.toHuman(mode)}
                            </a>
                          </li>
                        ))}
                      </ul>
                    </div>
                    <div
                      className={
                        "resize-control " +
                        (this.state.resizeMode === ResizeModes.NO ? "hide" : "")
                      }
                    >
                      <input
                        type="number"
                        step="100"
                        className="form-control"
                        onChange={this.handleResizeSizeChange}
                        value={this.state.resizeSize}
                      />
                      <span>{_("px")}</span>
                    </div>
                  </div>
                </div>
                {this.state.items.map((Item, i) => (
                  <div key={i} className="form-group">
                    <Item
                      taskInfo={this.state.taskInfo}
                      getFiles={this.props.getFiles}
                      filesCount={this.props.filesCount}
                    />
                  </div>
                ))}
              </div>
            ) : (
              ""
            )}
          </div>

          {this.state.editTaskFormLoaded ? (
            <React.Fragment>
              <div className="form-group">
                <label className="col-sm-2 control-label">
                  {_("S3 Endpoint")}
                </label>
                <div className="col-sm-10">
                  <input
                    disabled={
                      this.state.inReview || this.state.importingFromS3Url
                    }
                    onChange={this.handleChangeImportS3Url}
                    size="45"
                    type="text"
                    className="form-control"
                    placeholder="http://"
                    value={this.state.importS3Url}
                  />
                </div>
              </div>
              <div className="form-group">
                <label className="col-sm-2 control-label">
                  {_("S3 Access Key")}
                </label>
                <div className="col-sm-10">
                  <input
                    disabled={
                      this.state.inReview || this.state.importingFromS3Url
                    }
                    onChange={this.handleChangeImportS3Key}
                    size="45"
                    type="text"
                    className="form-control"
                    placeholder="Access Key"
                    value={this.state.importS3Key}
                    id="s3-access-key-input"
                  />
                </div>
              </div>
              <div className="form-group">
                <label className="col-sm-2 control-label">
                  {_("S3 Secret Key")}
                </label>
                <div className="col-sm-10">
                  <input
                    disabled={
                      this.state.inReview || this.state.importingFromS3Url
                    }
                    onChange={this.handleChangeImportS3Secret}
                    size="45"
                    type="text"
                    className="form-control"
                    placeholder="Secret Key"
                    value={this.state.importS3Secret}
                  />
                </div>
              </div>
              <div className="form-group">
                <label className="col-sm-2 control-label">
                  {_("S3 Bucket URL")}
                </label>
                <div className="col-sm-10">
                  <input
                    disabled={
                      this.state.inReview || this.state.importingFromS3Url
                    }
                    onChange={this.handleChangeImportS3Bucket}
                    size="45"
                    type="text"
                    className="form-control"
                    placeholder="s3://"
                    value={this.state.importS3Bucket}
                  />
                </div>
              </div>
              <div className="form-group">
                <div className="col-sm-offset-2 col-sm-10 text-right">
                  {this.props.onCancel !== undefined && (
                    <button
                      type="submit"
                      className="btn btn-danger"
                      onClick={this.cancel}
                      style={{ marginRight: 4 }}
                    >
                      <i className="glyphicon glyphicon-remove-circle"></i>{" "}
                      {_("Cancel")}
                    </button>
                  )}
                  {this.state.loading ? (
                    <button
                      type="submit"
                      className="btn btn-primary"
                      disabled={true}
                    >
                      <i className="fa fa-circle-notch fa-spin fa-fw"></i>
                      {_("Loading…")}
                    </button>
                  ) : (
                    <button
                      type="submit"
                      className="btn btn-primary"
                      onClick={this.save}
                      // disabled={this.props.filesCount < 1 || !filesCountOk}
                    >
                      <i className="glyphicon glyphicon-saved"></i>{" "}
                      {!this.state.inReview
                        ? _("Review")
                        : _("Start Processing")}
                    </button>
                  )}
                </div>
              </div>
            </React.Fragment>
          ) : (
            ""
          )}
        </div>
        <div className="form-horizontal">
          <ErrorMessage bind={[this, "error"]} />

          <button
            type="button"
            className="close theme-color-primary"
            title="Close"
            onClick={this.cancel}
          >
            <span aria-hidden="true">&times;</span>
          </button>

          <button
            disabled={this.state.uploading}
            type="button"
            className="btn btn-primary"
            onClick={this.handleImportFromS3}
            ref={this.setRef("importFromS3Button")}
          >
            <i className="glyphicon glyphicon-cloud-download"></i>
            {_("Import From S3")}
          </button>

          {this.state.typeS3Url ? (
            <div className="form-group">
              <button
                onClick={this.handleConfirmImportS3Url}
                disabled={
                  this.state.importS3Url.length < 4 ||
                  this.state.importingFromS3Url
                }
                className="btn-import btn btn-primary"
              >
                <i className="glyphicon glyphicon-cloud-download"></i>{" "}
                {_("Import")}
              </button>
            </div>
          ) : (
            ""
          )}

          {this.state.uploading ? (
            <div>
              <UploadProgressBar {...this.state} />
              <button
                type="button"
                className="btn btn-danger btn-sm"
                onClick={this.cancelUpload}
              >
                <i className="glyphicon glyphicon-remove-circle"></i>
                {_("Cancel Upload")}
              </button>
            </div>
          ) : (
            ""
          )}
        </div>
      </div>
    );
  }
}
/*































*/

class NewTaskPanel extends React.Component {
  static defaultProps = {
    filesCount: 0,
    showResize: false,
  };

  static propTypes = {
    onSave: PropTypes.func.isRequired,
    onCancel: PropTypes.func,
    filesCount: PropTypes.number,
    showResize: PropTypes.bool,
    getFiles: PropTypes.func,
    suggestedTaskName: PropTypes.oneOfType([PropTypes.string, PropTypes.func]),
  };

  constructor(props) {
    super(props);
  }

  render() {
    let filesCountOk = true;
    if (this.taskForm && !this.taskForm.checkFilesCount(this.props.filesCount))
      filesCountOk = false;

    return (
      <div className="new-task-panel theme-background-highlight">
        <div className="form-horizontal">
          <div className={this.state.inReview ? "disabled" : ""}>
            <p>
              {interpolate(
                _(
                  "%(count)s files selected. Please check these additional options:"
                ),
                { count: this.props.filesCount }
              )}
            </p>

            {!filesCountOk ? (
              <div className="alert alert-warning">
                {interpolate(
                  _(
                    "Number of files selected exceeds the maximum of %(count)s allowed on this processing node."
                  ),
                  { count: this.taskForm.selectedNodeMaxImages() }
                )}
                <button
                  onClick={this.props.onCancel}
                  type="button"
                  className="btn btn-xs btn-primary redo"
                >
                  <span>
                    <i className="glyphicon glyphicon-remove-circle"></i>{" "}
                    {_("Cancel")}
                  </span>
                </button>
              </div>
            ) : (
              ""
            )}

            <EditTaskForm
              selectedNode={Storage.getItem("last_processing_node") || "auto"}
              onFormLoaded={this.handleFormTaskLoaded}
              onFormChanged={this.handleFormChanged}
              inReview={this.state.inReview}
              suggestedTaskName={this.props.suggestedTaskName}
              ref={(domNode) => {
                if (domNode) this.taskForm = domNode;
              }}
            />

            {this.state.editTaskFormLoaded && this.props.showResize ? (
              <div>
                <div className="form-group">
                  <label className="col-sm-2 control-label">
                    {_("Resize Images")}
                  </label>
                  <div className="col-sm-10">
                    <div className="btn-group">
                      <button
                        type="button"
                        className="btn btn-default dropdown-toggle"
                        data-toggle="dropdown"
                      >
                        {ResizeModes.toHuman(this.state.resizeMode)}{" "}
                        <span className="caret"></span>
                      </button>
                      <ul className="dropdown-menu">
                        {ResizeModes.all().map((mode) => (
                          <li key={mode}>
                            <a
                              href="javascript:void(0);"
                              onClick={this.setResizeMode(mode)}
                            >
                              <i
                                style={{
                                  opacity:
                                    this.state.resizeMode === mode ? 1 : 0,
                                }}
                                className="fa fa-check"
                              ></i>{" "}
                              {ResizeModes.toHuman(mode)}
                            </a>
                          </li>
                        ))}
                      </ul>
                    </div>
                    <div
                      className={
                        "resize-control " +
                        (this.state.resizeMode === ResizeModes.NO ? "hide" : "")
                      }
                    >
                      <input
                        type="number"
                        step="100"
                        className="form-control"
                        onChange={this.handleResizeSizeChange}
                        value={this.state.resizeSize}
                      />
                      <span>{_("px")}</span>
                    </div>
                  </div>
                </div>
                {this.state.items.map((Item, i) => (
                  <div key={i} className="form-group">
                    <Item
                      taskInfo={this.state.taskInfo}
                      getFiles={this.props.getFiles}
                      filesCount={this.props.filesCount}
                    />
                  </div>
                ))}
              </div>
            ) : (
              ""
            )}
          </div>

          {this.state.editTaskFormLoaded ? (
            <div className="form-group">
              <div className="col-sm-offset-2 col-sm-10 text-right">
                {this.props.onCancel !== undefined && (
                  <button
                    type="submit"
                    className="btn btn-danger"
                    onClick={this.cancel}
                    style={{ marginRight: 4 }}
                  >
                    <i className="glyphicon glyphicon-remove-circle"></i>{" "}
                    {_("Cancel")}
                  </button>
                )}
                {this.state.loading ? (
                  <button
                    type="submit"
                    className="btn btn-primary"
                    disabled={true}
                  >
                    <i className="fa fa-circle-notch fa-spin fa-fw"></i>
                    {_("Loading…")}
                  </button>
                ) : (
                  <button
                    type="submit"
                    className="btn btn-primary"
                    onClick={this.save}
                    disabled={this.props.filesCount < 1 || !filesCountOk}
                  >
                    <i className="glyphicon glyphicon-saved"></i>{" "}
                    {!this.state.inReview ? _("Review") : _("Start Processing")}
                  </button>
                )}
              </div>
            </div>
          ) : (
            ""
          )}
        </div>
      </div>
    );
  }
}

export default ImportFromS3TaskPanel;
