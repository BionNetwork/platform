###*
# Queue management
# pub/sub
###

Queue = (server, port, path) ->
  @server = server
  @port = port or false
  @path = path or false
  @notes = new Array
  return

###*
# Уведомления в браузере
# @type {{notes: Array, get: Function, add: Function}}
###

Notes = 
  notes: []
  get: (topic) ->
    if @notes[topic] then @notes[topic] else null
  add: (topic, note) ->
    @notes[topic] = note
    return
  clear: (topic) ->
    if @notes[topic]
      delete @notes[topic]
    return

###*
# Активные соединения для каналов
#
# @type {{channels: Array, get: Function, set: Function}}
###

Channels = 
  channels: []
  get: (topic) ->
    if @channels[topic] then @channels[topic] else null
  set: (topic, connection) ->
    @channels[topic] = connection
    return

###*
# Общий метод для подписки на каналы
#
# @param channel
# @param callback
###

Queue::subscribe = (channel, callback) ->
  self = this
  if !('WebSocket' of window)
    console.warn 'websockets not supported'
    return
  ws = if location.protocol == 'https:' then 'wss://' else 'ws://'
  sessionUrl = ws + @server
  if false != @port
    sessionUrl += ':' + @port
  if false != @path
    sessionUrl += @path
  conn = new (ab.Session)(sessionUrl, (->
    # default callback
    if undefined == callback

      callback = (topic, data) ->
        console.log topic, data
        return

    Channels.set channel, self
    conn.subscribe channel, callback
    return
  ), (->
    console.warn 'WebSocket connection closed'
    return
  ), 'skipSubprotocolCheck': true)
  @connection = conn
  return

###*
# Отписка от событий
#
# @param channel
###

Queue::unsubscribe = (channel) ->
  self = this
  url = undefined
  url = '/channel/unsubscribe/' + channel
  if self.connection
    $.post url, { id: channel }, ((response) ->
      if !('WebSocket' of window)
        console.warn 'websockets not supported'
      else
        self.connection.unsubscribe channel
        Notes.clear channel
      console.log 'unsubscribed from ' + channel
      return
    ), 'json'
  return

###*
# Подписка на etl процессы
#
# @param topic
# @param data
###

Queue::etlload = (topic, data) ->
  note = Notes.get(topic)
  if data.event == 'process'
    if null == note
      note = $.sticky(data.message + ' ' + data.percent + '%',
        autoclose: false
        position: 'bottom-right'
        sticky: data.channel
        closeCallback: ->
          Channels.get(topic).unsubscribe topic
          return
      )
      Notes.add topic, note
    else
      $.stickyUpdate note.id, data.message + ' ' + data.percent + '%'
  # при завершении отписываемся от канала
  if data.event == 'finish' and data.close != undefined
    Channels.get(topic).unsubscribe topic
    if note
      $.stickyClose note.id, 300
  if 'Notification' of window
    # notification window
    Notification.requestPermission (permission) ->
      message = null
      notification = undefined
      if data.event == 'start'
        notification = new Notification('Задача поставлена в очередь',
          body: 'Обработка началась'
          tag: topic)
      if data.event == 'finish' and data.close != undefined
        notification = new Notification('Обработка завершилась',
          body: 'Обработка задачи №' + data.id + ' завершилась'
          tag: topic)
        message = 'Обработка задачи №' + data.id + ' завершилась'
      if data.event == 'error'
        notification = new Notification('Ошибка в обработке',
          body: data.message
          tag: topic)
        message = 'Ошибка в обработке\n' + data.message
      if undefined != notification
        setTimeout (->
          notification.close()
          #closes the notification
          return
        ), 2000

        notification.onerror = ->
          # fallback если отключены нотификации
          if message
            if null == note
              note = $.sticky(message,
                autoclose: 5000
                position: 'bottom-right'
                sticky: data.channel)
              Notes.add topic, note
            else
              $.stickyUpdate note.id, message
          return

      return
  return

