
$(document).ready(function(){

    var tasksUl = $('#user_tasks_bar');

    if(tasksUl.length){
        var taskTmpl = _.template($('#tasks_progress').html());

        $.get(tasksUl.data('url'), {}, function(data){
            tasksUl.append(taskTmpl({data: data.tasks}));

            _.each(data.tasks, function(el){
                var ws = new WebSocket(
                    "ws://"+tasksUl.data('host')+"user/"+
                        data.userId+"/task/"+el);
                ws.onmessage = function (evt){
                    $('#task-text-'+el).text(evt.data+'%')
                    $('#task-measure-'+el).css('width', evt.data+'%')
                };
            });
        });
    }
});