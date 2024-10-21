import React from 'react';
import '../css/AssetDownloadButtons.scss';
import AssetDownloads from '../classes/AssetDownloads';
import PropTypes from 'prop-types';
import ExportAssetDialog from './ExportAssetDialog';
import { _ } from '../classes/gettext';
import Workers from '../classes/Workers';

class AssetDownloadButtons extends React.Component {
    static defaultProps = {
        disabled: false,
        direction: "down", // or "up",
        buttonClass: "btn-primary",
        task: null,
        showLabel: true
    };

    static propTypes = {
        disabled: PropTypes.bool,
        task: PropTypes.object.isRequired,
        direction: PropTypes.string,
        buttonClass: PropTypes.string,
        showLabel: PropTypes.bool,
        onModalOpen: PropTypes.func,
        onModalClose: PropTypes.func
    };

    constructor(props){
        super();

        this.state = {
            exportDialogProps: null
        }
    }

    onHide = () => {
        this.setState({exportDialogProps: null});
        if (this.props.onModalClose) this.props.onModalClose();
    }

    handleDownloadZip = (asset) => {
        this.downloadZip(asset.downloadUrl(this.props.task.project, this.props.task.id))
    }

    handleDownloadBackup = () => {
        this.downloadZip(`/api/projects/${this.props.task.project}/tasks/${this.props.task.id}/backup`)
    }

    downloadZip = (url) => {
        $.ajax({
            type: 'GET',
            url
        }).done(result => {
            if (result.celery_task_id){
                Workers.waitForCompletion(result.celery_task_id, error => {
                    if (error){
                        console.error(error)
                    } else {
                        Workers.downloadFile(result.celery_task_id, result.filename);
                    }
                });
            }
        }).fail(error => {
            error = (error.responseJSON || {})[0] || JSON.stringify(error);
            console.error(error)
        });
    }

    render(){
        const assetDownloads = AssetDownloads.only(this.props.task.available_assets);

        return (<div className={"asset-download-buttons " + (this.props.showLabel ? "btn-group" : "") + " " + (this.props.direction === "up" ? "dropup" : "")}>
          
          {this.state.exportDialogProps ? 
            <ExportAssetDialog task={this.props.task}
                               asset={this.state.exportDialogProps.asset}
                               exportFormats={this.state.exportDialogProps.exportFormats}  
                               exportParams={this.state.exportDialogProps.exportParams}
                               onHide={this.onHide}
                               assetLabel={this.state.exportDialogProps.assetLabel}
            /> 
            : ""}

          <button type="button" className={"btn btn-sm " + this.props.buttonClass} disabled={this.props.disabled} data-toggle="dropdown">
            <i className="glyphicon glyphicon-download"></i>{this.props.showLabel ? " " + _("Download Assets") : ""}
          </button>
          {this.props.showLabel ? 
          <button type="button" className={"btn btn-sm dropdown-toggle " + this.props.buttonClass} data-toggle="dropdown" disabled={this.props.disabled}>
                <span className="caret"></span>
          </button> : ""}
          <ul className="dropdown-menu">
            {assetDownloads.map((asset, i) => {
                if (asset.separator){
                    return (<li key={i} className="divider"></li>);
                }else{
                    let onClick = undefined;
                    let buttonHref = asset.downloadUrl(this.props.task.project, this.props.task.id)
                    const isZipDownload = buttonHref.endsWith('.zip') || buttonHref.endsWith('/backup')

                    if (asset.exportFormats){
                        onClick = e => {
                            e.preventDefault();
                            this.setState({exportDialogProps: {
                                asset: asset.exportId(),
                                exportFormats: asset.exportFormats,
                                exportParams: asset.exportParams,
                                assetLabel: asset.label
                            }});
                            if (this.props.onModalOpen) {
                                this.props.onModalOpen();
                            } else if (isZipDownload) {
                                this.handleDownloadZip(asset)
                            }
                        }
                    } else {
                        onClick = e => {
                            e.preventDefault()

                            if (isZipDownload) {
                                this.handleDownloadZip(asset)
                            }
                        }
                    }

                    
                    if (isZipDownload) {
                        buttonHref = 'javascript:void(0);'
                    }

                    return (<li key={i}>
                            <a href={buttonHref} onClick={onClick}><i className={asset.icon + " fa-fw"}></i> {asset.label}</a>
                        </li>);
                }
            })}
            <li>
                <a href="javascript:void(0);" onClick={this.handleDownloadBackup}><i className="fa fa-file-download fa-fw"></i> {_("Backup")}</a>
            </li>
          </ul>
        </div>);
    }
}

export default AssetDownloadButtons;
