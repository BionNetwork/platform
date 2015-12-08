

var Queue2 = function(server, port, path){
    this.server = server;
    this.port = port || false;
    this.path = path || false;
    this.notes = new Array();
}


Queue2.prototype.subscribe = function(channel){

    var self = this;

    function etlLoadCallback(r){
        var tasksUl = $('#user_tasks_bar'),
            resp = JSON.parse(r),
            taskId = resp.taskId;


        // work with progressbar
        if(!$('#task-li-'+taskId).length){
            var taskTmpl = _.template($('#tasks_progress').html());
            tasksUl.append(taskTmpl({data: [taskId ]}));
        }
        $('#task-text-'+taskId).text(resp.percent+'%');
        $('#task-measure-'+taskId).css('width', resp.percent+'%')


        // work with notifications
        if ('Notification' in window){
            Notification.requestPermission(function(permission){
                if (resp.event == 'start'){
                    var notification = new Notification(
                        "Задача поставлена в очередь!", {body:'Обработка началась!'});
                }
                if (resp.event == 'finish'){
                    var notification = new Notification("Обработка завершилась!",
                        {body : 'Обработка задачи №' + resp.taskId + ' завершилась!'})
                }
                if (resp.event == 'error'){
                    var notification = new Notification(
                    'Ошибка в обработке!', {body: resp.message})
                }
            });
        }

        if (resp.event == 'finish'){
            self.conn.close();
        }
    };

    if (!('WebSocket' in window)) {
        console.warn("websockets not supported");
        return;
    }
    var ws = location.protocol == 'https:' ? 'wss://' : 'ws://',
        sessionUrl = ws + this.server;

    if (false != this.port) {
        sessionUrl += ':' + this.port;
    }
    if (false != this.path) {
        sessionUrl += this.path;
    }

    self.conn = new autobahn.Connection({
        url: sessionUrl,
        realm: 'realm1'
        });

    self.conn.open()

    self.conn.onopen = function (session){
        // subscribe to channel
        session.subscribe(channel, etlLoadCallback);

        // call a remote procedure(for publishing)
        session.call('set_publish_channel', [channel]);
    }

    self.conn.onclose = function (session){
        console.log('close');
    }
}
