import $ from 'jquery';

const waitForCompletion = (celery_task_id, cb, checkUrl = "/api/workers/check/") => {
    let errorCount = 0;
    let url = checkUrl + celery_task_id;

    const check = () => {
        $.ajax({
            type: 'GET',
            url
        }).done(result => {
            if (result.error){
            cb(result.error);
            }else if (result.ready){
            cb();
            }else{
            // Retry
            setTimeout(() => check(), 2000);
            }
        }).fail(error => {
            console.warn(error);
            if (errorCount++ < 10) setTimeout(() => check(), 2000);
            else cb(error.statusText);
        });
    };

    check();
}

const getOutput = (celery_task_id, cb, getUrl = "/api/workers/get/") => {
    let url = getUrl + celery_task_id;
    $.ajax({
        type: 'GET',
        url: url
    }).done(result => {
        if (result.error) cb(result.error);
        else if (result.output !== undefined) cb(null, result.output);
        else cb(new Error("Invalid response: " + JSON.stringify(result)));
    }).fail(cb);
}

export default {
    waitForCompletion,
    downloadFile: (celery_task_id, filename = "") => {
        window.location.href = `/api/workers/get/${celery_task_id}?filename=${filename}`;
    },
    getOutput,
    waitAndGetResult: (celery_task_id, cb, checkUrl = "/api/workers/check/", getUrl = "/api/workers/get/") => {
        waitForCompletion(celery_task_id, 
            () => getOutput(celery_task_id, cb, getUrl),
            checkUrl)
    }
};

