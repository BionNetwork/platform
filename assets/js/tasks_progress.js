
$(document).ready(function(){

    var tasksUl = $('#user_tasks_bar');

    if(tasksUl.length){

        $.get(tasksUl.data('url'), {}, function(data){
            console.log('Channels: ', data.channels);

            _.each(data.channels, function(channel){
                var q = new Queue2(
                            tasksUl.data('host'), tasksUl.data('port'), '/ws');
                    // подписка на канал
                    q.subscribe(channel)
            });
        });
    }
});
