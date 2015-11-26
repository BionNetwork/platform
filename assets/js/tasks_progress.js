
$(document).ready(function(){

    var tasksUl = $('#user_tasks_bar');

    if(tasksUl.length){
        var taskTmpl = _.template($('#tasks_progress').html());

        $.get(tasksUl.data('url'), {}, function(data){

            _.each(data.channels, function(channel){
                var ws = new WebSocket(
                    "ws://"+tasksUl.data('host')+"channel/"+channel);

                ws.onopen = function(){
                };
                ws.onmessage = function (evt){
                    var data = JSON.parse(evt.data),
                        taskId = data.taskId;

                    if(!$('#task-li-'+taskId).length){
                        var taskTmpl = _.template($('#tasks_progress').html());
                        tasksUl.append(taskTmpl({data: [taskId ]}));
                    }
                    $('#task-text-'+taskId).text(data.percent+'%');
                    $('#task-measure-'+taskId).css('width', data.percent+'%');
                };
                ws.onclose = function(){
                };
            });
        });
    }
});