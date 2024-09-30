import { _ } from "./gettext";

const CANCEL = 1,
  REMOVE = 2,
  RESTART = 3,
  RESIZE = 4,
  IMPORT = 5,
  IMPORT_FROM_S3 = 6,
  IMPORT_FROM_S3_WITH_RESIZE = 7;

let pendingActions = {
  [CANCEL]: {
    descr: _("Canceling..."),
  },
  [REMOVE]: {
    descr: _("Deleting..."),
  },
  [RESTART]: {
    descr: _("Restarting..."),
  },
  [RESIZE]: {
    descr: _("Resizing images..."),
  },
  [IMPORT]: {
    descr: _("Importing..."),
  },
  [IMPORT_FROM_S3]: {
    descr: _("Downloading from s3..."),
  },
  [IMPORT_FROM_S3_WITH_RESIZE]: {
    descr: _("Downloading from s3..."),
  },
};

export default {
  CANCEL: CANCEL,
  REMOVE: REMOVE,
  RESTART: RESTART,
  RESIZE: RESIZE,
  IMPORT: IMPORT,
  IMPORT_FROM_S3: IMPORT_FROM_S3,
  IMPORT_FROM_S3_WITH_RESIZE: IMPORT_FROM_S3_WITH_RESIZE,

  description: function (pendingAction) {
    if (pendingActions[pendingAction])
      return pendingActions[pendingAction].descr;
    else return "";
  },
};
