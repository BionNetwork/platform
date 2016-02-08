/**
 * Queue management
 * pub/sub
 */
var Channels, Notes, Queue;

Queue = function(server, port, path) {
  this.server = server;
  this.port = port || false;
  this.path = path || false;
  this.notes = new Array;
};


/**
 * Уведомления в браузере
 * @type {{notes: Array, get: Function, add: Function}}
 */

Notes = {
  notes: [],
  get: function(topic) {
    if (this.notes[topic]) {
      return this.notes[topic];
    } else {
      return null;
    }
  },
  add: function(topic, note) {
    this.notes[topic] = note;
  },
  clear: function(topic) {
    if (this.notes[topic]) {
      delete this.notes[topic];
    }
  }
};


/**
 * Активные соединения для каналов
#
 * @type {{channels: Array, get: Function, set: Function}}
 */

Channels = {
  channels: [],
  get: function(topic) {
    if (this.channels[topic]) {
      return this.channels[topic];
    } else {
      return null;
    }
  },
  set: function(topic, connection) {
    this.channels[topic] = connection;
  }
};


/**
 * Общий метод для подписки на каналы
#
 * @param channel
 * @param callback
 */

Queue.prototype.subscribe = function(channel, callback) {
  var conn, self, sessionUrl, ws;
  self = this;
  if (!('WebSocket' in window)) {
    console.warn('websockets not supported');
    return;
  }
  ws = location.protocol === 'https:' ? 'wss://' : 'ws://';
  sessionUrl = ws + this.server;
  if (false !== this.port) {
    sessionUrl += ':' + this.port;
  }
  if (false !== this.path) {
    sessionUrl += this.path;
  }
  conn = new ab.Session(sessionUrl, (function() {
    if (void 0 === callback) {
      callback = function(topic, data) {
        console.log(topic, data);
      };
    }
    Channels.set(channel, self);
    conn.subscribe(channel, callback);
  }), (function() {
    console.warn('WebSocket connection closed');
  }), {
    'skipSubprotocolCheck': true
  });
  this.connection = conn;
};


/**
 * Отписка от событий
#
 * @param channel
 */

Queue.prototype.unsubscribe = function(channel) {
  var self, url;
  self = this;
  url = void 0;
  url = '/channel/unsubscribe/' + channel;
  if (self.connection) {
    $.post(url, {
      id: channel
    }, (function(response) {
      if (!('WebSocket' in window)) {
        console.warn('websockets not supported');
      } else {
        self.connection.unsubscribe(channel);
        Notes.clear(channel);
      }
      console.log('unsubscribed from ' + channel);
    }), 'json');
  }
};


/**
 * Подписка на etl процессы
#
 * @param topic
 * @param data
 */

Queue.prototype.etlload = function(topic, data) {
  var note;
  note = Notes.get(topic);
  if (data.event === 'process') {
    if (null === note) {
      note = $.sticky(data.message + ' ' + data.percent + '%', {
        autoclose: false,
        position: 'bottom-right',
        sticky: data.channel,
        closeCallback: function() {
          Channels.get(topic).unsubscribe(topic);
        }
      });
      Notes.add(topic, note);
    } else {
      $.stickyUpdate(note.id, data.message + ' ' + data.percent + '%');
    }
  }
  if (data.event === 'finish' && data.close !== void 0) {
    Channels.get(topic).unsubscribe(topic);
    if (note) {
      $.stickyClose(note.id, 300);
    }
  }
  if ('Notification' in window) {
    Notification.requestPermission(function(permission) {
      var message, notification;
      message = null;
      notification = void 0;
      if (data.event === 'start') {
        notification = new Notification('Задача поставлена в очередь', {
          body: 'Обработка началась',
          tag: topic
        });
      }
      if (data.event === 'finish' && data.close !== void 0) {
        notification = new Notification('Обработка завершилась', {
          body: 'Обработка задачи №' + data.id + ' завершилась',
          tag: topic
        });
        message = 'Обработка задачи №' + data.id + ' завершилась';
      }
      if (data.event === 'error') {
        notification = new Notification('Ошибка в обработке', {
          body: data.message,
          tag: topic
        });
        message = 'Ошибка в обработке\n' + data.message;
      }
      if (void 0 !== notification) {
        setTimeout((function() {
          notification.close();
        }), 2000);
        notification.onerror = function() {
          if (message) {
            if (null === note) {
              note = $.sticky(message, {
                autoclose: 5000,
                position: 'bottom-right',
                sticky: data.channel
              });
              Notes.add(topic, note);
            } else {
              $.stickyUpdate(note.id, message);
            }
          }
        };
      }
    });
  }
};
