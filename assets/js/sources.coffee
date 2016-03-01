chosenTables =
colsTemplate =
colsHeaders =
joinWinRow =
joinWin =
selectedRow =
dataWorkspace =
loader =
initDataTable =
closeUrl =
dataWindow = null
# событие на закрытие модального окна

confirmAlert = (message) ->
  $.confirm
    width: '100px'
    text: message
    title: 'Внимание!'
    confirmButtonClass: 'btn-danger'
    cancelButtonClass: 'hidden'
    confirmButton: 'Ок'
  return

checkConnection = ->
  form = $('#conn_form')
  formData = new FormData(form[0])
  url = form.attr('data-url')
  $.validator.messages.required = 'Обязательное поле!'
  form.validate
    rules:
      port: number: true
      password: required: false
    messages: port: number: 'Введите целое число!'
  $.each form.find('.border-red'), (i, el) ->
    console.log el
    $(el).removeClass 'border-red'
    return
  if !form.valid()
    $.each form.validate().errorList, (i, el2) ->
      $(el2.element).addClass 'border-red'
      return
    return false
  $.ajax
    url: url
    data: formData
    processData: false
    contentType: false
    type: 'POST'
    success: (result) ->
      if result.status == 'error'
        $.confirm
          text: result.message or 'Подключение не удалось!'
          title: 'Внимание'
          confirmButtonClass: 'btn-danger'
          cancelButtonClass: 'hidden'
          confirmButton: 'Ок'
      else if result.status == 'success'
        $.confirm
          width: '100px'
          text: result.message or 'Подключение удалось!'
          title: 'Внимание'
          cancelButtonClass: 'hidden'
          confirmButton: 'Ок'
      return
  return

search = ->
  `var search`
  etlUrl = $('#source_table').attr('data-url')
  search = $('#search').val()
  document.location = etlUrl + '?search=' + search
  return

removeSource = (url) ->
  $.confirm
    text: 'Вы действительно хотите удалить источник?'
    confirm: (button) ->
      $.post url, { csrfmiddlewaretoken: csrftoken }, (data) ->
        window.location = data.redirect_url
        return
      return
    title: 'Удаление источника'
    confirmButton: 'Удалить'
    cancelButton: 'Отмена'
  return

createSettigns = ->
  $.validator.messages.required = 'Обязательное поле!'
  if !$('#conn_form').valid()
    return
  $('#settings-window').modal 'show'
  return

saveNewSource = (save_url) ->
  connection_form = $('#conn_form')
  formData = new FormData(connection_form[0])
  url = save_url or connection_form.attr('data-save-url')
  formData.append 'cdc_type', $('#cdc_select').val()
  $.validator.messages.required = 'Обязательное поле!'
  if !connection_form.valid()
    return false
  $.ajax
    url: url
    data: formData
    processData: false
    contentType: false
    type: 'POST'
    success: (result) ->
      $('#settings-window').modal 'hide'
      if result.status == 'error'
        confirmAlert result.message
      else
        window.location = result.redirect_url
      return
  return

closeSettings = ->
  $('#settings-window').modal 'hide'
  return

getConnectionData = (dataUrl, closingUrl) ->
  closeUrl = closingUrl
  colsTemplate = _.template($('#table-cols').html())
  colsHeaders = _.template($('#cols-headers').html())
  selectedRow = _.template($('#selected-rows').html())
  initDataTable = _.template($('#datatable-init').html())
  joinWinRow = _.template($('#join-win-row').html())
  dataWindow = $('#modal-data')
  joinWin = $('#join-window')
  loader = $('#loader')
  loader.hide()
  $.get dataUrl, { csrfmiddlewaretoken: csrftoken }, (res) ->
    rowsTemplate = _.template($('#database-rows').html())
    $('#databases').html rowsTemplate(data: res.data)
    chosenTables = $('#chosenTables')
    dataWorkspace = $('#data-workspace')
    chosenTables.html ''
    dataWorkspace.html initDataTable
    dataWindow.modal 'show'
    $('#button-toRight').addClass 'disabled'
    $('#button-allToRight').addClass 'disabled'
    $('#button-toLeft').addClass 'disabled'
    $('#button-allToLeft').addClass 'disabled'
    if res.status == 'error'
      confirmAlert res.message
    return
  return

checkTable = (table) ->
  tableRow = $('#' + table)
  if tableRow.hasClass('table-selected')
    tableRow.removeClass 'table-selected'
    $('#button-toRight').addClass 'disabled'
  checkboxes = $('.checkbox-table:checked')
  if checkboxes.length
    $('#button-allToRight').removeClass 'disabled'
  else
    $('#button-allToRight').addClass 'disabled'
  return

setActive = (table) ->
  tableRow = $('#' + table)
  if tableRow.hasClass('table-selected')
    tableRow.removeClass 'table-selected'
    $('.checkbox-table').prop 'checked', false
    $('#button-toRight').addClass 'disabled'
    checkboxes = $('.checkbox-table:checked')
    if !checkboxes.length
      $('#button-allToRight').addClass 'disabled'
  else
    $('.checkbox-table').prop 'checked', false
    $('.table-selected').removeClass 'table-selected'
    tableRow.addClass 'table-selected'
    tableRow.find('input[type="checkbox"]').prop 'checked', true
    $('#button-toRight').removeClass 'disabled'
  return

checkRightCheckboxes = ->
  if $('.right-chbs:checked').length
    $('#button-toLeft').removeClass 'disabled'
  else
    $('#button-toLeft').addClass 'disabled'
  return

drawTables = (data) ->
  chosenTables.html ''
  if data[0].is_root
    chosenTables.append colsTemplate(row: data[0])
    data = data.slice(1)
  _.each data, (el) ->
    $('#for-' + el.dest + '-childs').append colsTemplate(row: el)
    return
  return

getColumns = (url, dict) ->
  $.get url, dict, (res) ->
    if res.status == 'error'
      confirmAlert res.message
    else
      drawTables res.data
      $('#data-table-headers').html ''
      $('#data-table-headers').append colsHeaders(data: res.data)
      $('#button-allToLeft').removeClass 'disabled'
    return
  return

hasWithoutBinds = ->
  # если есть талица без связи, то внимание
  if $('.without_bind').length
    confirmAlert 'Обнаружены ошибки в связях! ' + 'Выберите правильную связь у таблицы, либо удалите ее!'
    return true
  false

tableToRight = (url) ->
  if $('#button-toRight').hasClass('disabled')
    return
  if hasWithoutBinds()
    return
  selectedTable = $('div.table-selected')
  if selectedTable.length and !$('#' + selectedTable.attr('id') + 'Cols').length
    dataWorkspace.find('.result-col').remove()
    getColumns url,
      csrfmiddlewaretoken: csrftoken
      host: selectedTable.attr('data-host')
      db: selectedTable.attr('data-db')
      tables: JSON.stringify([ selectedTable.attr('data-table') ])
  return

tablesToRight = (url) ->
  if $('#button-allToRight').hasClass('disabled')
    return
  if hasWithoutBinds()
    return
  divs = $('.checkbox-table:checked').closest('div')
  dict =
    csrfmiddlewaretoken: csrftoken
    host: divs.attr('data-host')
    db: divs.attr('data-db')
  tables = divs.map(->
    el = $(this)
    id = el.attr('id')
    if !$('#' + id + 'Cols').length
      return el.attr('data-table')
    return
  ).get()
  if tables.length
    dataWorkspace.find('.result-col').remove()
    dict['tables'] = JSON.stringify(tables)
    getColumns url, dict
  return

addCol = (tName, colName) ->
  if !$('#head-' + tName + '-' + colName + ':visible').length
    $('#for-head-' + tName + '-' + colName).css 'font-weight', 'bold'
    col = $('#head-' + tName + '-' + colName)
    ths = $('#data-table-headers').find('th')
    index = ths.index(col)
    workspaceRows = dataWorkspace.find('table tr').not(':first')
    $(workspaceRows).each (trIndex, tRow) ->
      if !index
        $(tRow).prepend '<td></td>'
      else
        $('<td></td>').insertAfter $(tRow).find('td').eq(index - 1)
      return
    col.show()
    col.addClass 'data-table-column-header'
  return

delCol = (id) ->
  if $('#' + id + ':visible').length
    $('#for-' + id).css 'font-weight', 'normal'
    $('#' + id).hide()
    $('#' + id).removeClass 'data-table-column-header'
    ths = $('#data-table-headers').find('th')
    header = $('#' + id)
    index = ths.index(header)
    workspaceRows = dataWorkspace.find('table tr').not(':first')
    $(workspaceRows).each (trIndex, tRow) ->
      $(tRow).find('td').eq(index).remove()
      if $(tRow).length == 0
        $(tRow).remove()
      return
  return

getSourceInfo = ->
  source = $('#databases>div')
  {
    'host': source.data('host')
    'db': source.data('db')
  }

tableToLeft = (url) ->
  if $('#button-toLeft').hasClass('disabled')
    return
  # чекбоксы с дочерними чекбоксами
  checked = $('.right-chbs:checked').closest('.table-part').find('.right-chbs')
  divs = checked.siblings('div').find('div')
  indexes = []
  ths = $('#data-table-headers').find('th').not(':hidden')
  $.each divs, (i, el) ->
    header = $('#col-' + $(this).data('table') + '-' + $(this).data('col'))
    indexes.push ths.index(header)
    header.remove()
    return
  workspaceRows = dataWorkspace.find('table tr').not(':first')
  reversed = indexes.reverse()
  # удаляем все строки данных
  workspaceRows.remove()
  # удаляем ячейки по индексам (функция работает некорректно)
  #    $(workspaceRows).each(function(trIndex, tRow){
  #        $.each(reversed, function(i, el){
  #            $(tRow).find("td").eq(el).remove();
  #        });
  #        if ($(tRow).length == 0) {
  #            $(tRow).remove();
  #        }
  #    });
  selTables = checked.closest('.table-part')
  tablesToDelete = []
  $.each selTables, (i, el) ->
    $(this).closest('.table-part').find('.table-part').remove()
    return
  checked2 = $('.right-chbs:checked')
  selTables2 = checked2.closest('.table-part')
  $.each selTables2, (i, el) ->
    tablesToDelete.push $(this).data('table')
    $(this).remove()
    return
  info = getSourceInfo()
  info['tables'] = JSON.stringify(tablesToDelete)
  $.get url, info, (res) ->
    if res.status == 'error'
      confirmAlert res.message
    return
  checkRightCheckboxes()
  if !chosenTables.children().length
    $('#button-allToLeft').addClass 'disabled'
  return

tablesToLeft = (url) ->
  if $('#button-allToLeft').hasClass('disabled')
    return
  info = getSourceInfo()
  # удалять ddl надо
  info['delete_ddl'] = true
  $.get url, info, (res) ->
    if res.status == 'error'
      confirmAlert res.message
    else
      chosenTables.html ''
      dataWorkspace.html initDataTable
      $('#button-toLeft').addClass 'disabled'
      $('#button-allToLeft').addClass 'disabled'
    return
  return

refreshData = (url) ->
  if hasWithoutBinds()
    return
  source = $('#databases>div')
  colsInfo =
    'host': source.data('host')
    'db': source.data('db')
  cols = dataWorkspace.find('.data-table-column-header')
  array = cols.map(->
    el = $(this)
    {
      'table': el.data('table')
      'col': el.data('col')
    }
  ).get()
  if array.length
    colsInfo['cols'] = JSON.stringify(array)
    # удаляем все ячейки с данными
    dataWorkspace.find('table tr').not(':first').remove()
    loader.show()
    dataWorkspace.parent('div').css 'background-color', '#ddd'
    $.post url, colsInfo, (res) ->
      if res.status == 'error'
        confirmAlert res.message
      else
        tableData = dataWorkspace.find('table > tbody')
        tableData.append selectedRow(data: res.data)
      loader.hide()
      dataWorkspace.parent('div').css 'background-color', 'white'
      return
  return

insertJoinRows = (data, parent, child, joinRows) ->
  $.each data.good_joins, (i, join) ->
    newRow = joinWinRow(
      parentCols: data.columns[parent]
      childCols: data.columns[child]
      i: i
      error: false)
    joinRows.append $(newRow)
    $('[name="joinradio"][value=' + join['join']['type'] + ']').prop 'checked', true
    $('.with-select-' + i).find('select[name="parent"]').val join['left']['column']
    $('.with-select-' + i).find('select[name="child"]').val join['right']['column']
    $('.with-select-' + i).find('select[name="joinType"]').val join['join']['value']
    return
  goodLen = data.good_joins.length
  $.each data.error_joins, (i, join) ->
    j = i + goodLen
    newRow = joinWinRow(
      parentCols: data.columns[parent]
      childCols: data.columns[child]
      i: j
      error: true)
    joinRows.append $(newRow)
    $('[name="joinradio"][value=' + join['join']['type'] + ']').prop 'checked', true
    $('.with-select-' + j).find('select[name="parent"]').val join['left']['column']
    $('.with-select-' + j).find('select[name="child"]').val join['right']['column']
    $('.with-select-' + j).find('select[name="joinType"]').val join['join']['value']
    return
  return

showJoinWindow = (url, parent, child, isWithoutBind) ->
  info = getSourceInfo()
  info['parent'] = parent
  info['child_bind'] = child
  warn = $('#table-part-' + child + '>div:first').find('.without_bind')
  info['has_warning'] = if warn.length then true else false
  $.get url, info, (res) ->
    if res.status == 'error'
      confirmAlert res.message
    else
      joinRows = $('#joinRows')
      data = res.data
      joinRows.html ''
      joinRows.data 'table-left', parent
      joinRows.data 'table-right', child
      # последняя таблица без связей
      if !data.good_joins.length and !data.error_joins.length
        joinRows.append joinWinRow(
          parentCols: data.columns[parent]
          childCols: data.columns[child]
          i: 0
          error: false)
      else
        insertJoinRows data, parent, child, joinRows
      $('#parentLabel').text parent
      $('#childLabel').text child
      joinWin.modal 'show'
    return
  return

addNewJoin = ->
  joinRows = $('#joinRows')
  parentCols = []
  childCols = []
  parOptions = joinWin.find('select[name="parent"]').first().find('option')
  wobOptions = joinWin.find('select[name="child"]').first().find('option')
  $.each parOptions, (i, el) ->
    parentCols.push $(el).attr('value')
    return
  $.each wobOptions, (i, el) ->
    childCols.push $(el).attr('value')
    return
  joinRows.append joinWinRow(
    parentCols: parentCols
    childCols: childCols
    i: 0
    error: false)
  return

deleteJoins = ->
  $('.checkbox-joins:checked').closest('.join-row').remove()
  return

saveJoins = (url) ->
  joins = $('.join-row')
  joinsArray = []
  if !joins.length
    confirmAlert 'Пожалуйста, выберите связь!'
    return
  $.each joins, (i, row) ->
    selects = $(row).find('select')
    vals = []
    $.each selects, (j, sel) ->
      vals.push $(sel).val()
      return
    joinsArray.push vals
    return
  joinsSet = new Set
  # избавляемся от дублей джойнов
  $.each joinsArray, (i, row) ->
    joinsSet.add row[0] + row[2]
    return
  if joinsArray.length != joinsSet.size
    confirmAlert 'Имеются дубли среди связей, пожалуйста удалите лишнее!'
    return
  joinRows = $('#joinRows')
  info = getSourceInfo()
  info['joins'] = JSON.stringify(joinsArray)
  info['left'] = joinRows.data('table-left')
  info['right'] = joinRows.data('table-right')
  info['joinType'] = $('[name="joinradio"]:checked').val()
  $.get url, info, (res) ->
    if res.status == 'error'
      confirmAlert res.message
    else
      joinWin.modal 'hide'
      rightTableArea = $('#table-part-' + joinRows.data('table-right') + '>div:first')
      rel = rightTableArea.find('.relation')
      warn = $('<span class="without_bind" style="color:red;">!!!</span>')
      # если новые джойны неверны, добавляем красное, если еще не было
      if res.data.has_error_joins == true
        if !rel.find('.without_bind').length
          rel.append warn
      else
        # res.data.has_error_joins == false
        rightTableArea.find('.without_bind').remove()
      # если совсем нет ошибок ни у кого, то перерисуем дерево,
      # на всякий пожарный
      if !$('.without_bind').length
        drawTables res.data.draw_table
    return
  return

closeJoins = ->
  joinWin.modal 'hide'
  return

startLoading = (userId, loadUrl) ->
  info = getSourceInfo()
  tables = new Set
  cols = dataWorkspace.find('.data-table-column-header')
  array = cols.map(->
    el = $(this)
    tables.add el.data('table')
    {
      'table': el.data('table')
      'col': el.data('col')
    }
  ).get()
  if hasWithoutBinds()
    return
  if !array.length
    confirmAlert 'Выберите таблицы для загрузки!'
    return
  tablesArray = []
  tables.forEach (el) ->
    tablesArray.push el
    return
  info['cols'] = JSON.stringify(array)
  info['tables'] = JSON.stringify(tablesArray)
  $.post loadUrl, info, (response) ->
    if response.status == 'error'
      confirmAlert response.message
    else
      # признак того, что окно закрылось при нажатии кнопки
      dataWindow.data 'load', true
      dataWindow.modal 'hide'
      # clear data
      dataWindow.data 'load', false
      channels = response.data['channels']
      tasksUl = $('#user_tasks_bar')
      _.each channels, (channel) ->
        q = new Queue2(tasksUl.data('host'), tasksUl.data('port'), '/ws')
        # подписка на канал
        q.subscribe channel
        return
    return
  return

renameColumn = (headerId) ->
    $('#text-' + headerId).hide()
    $('#cancel-' + headerId).show()
    $('#input-' + headerId).show()
    return

cancelRenameColumn = (headerId) ->
    $('#text-' + headerId).show();
    $('#input-' + headerId).hide();
    $('#cancel-' + headerId).hide();
    return

saveColumnName = (headerId, event, url) ->
    # on Enter press
    if event.keyCode == 13
        head = $('#' + headerId)
        text = $('#text-' + headerId)
        input = $('#input-' + headerId)
        cancel = $('#cancel-' + headerId)
        table = head.data('table')
        realColumnName = head.data('col')
        newColumnName = input.val()
        text.text(newColumnName)
        text.show()
        input.hide()
        cancel.hide()
        info = getSourceInfo()
        info['table'] = table
        info['column'] = realColumnName
        info['title'] = newColumnName
        $.post(url, info, (response) -> )
    return


$('#modal-data').on 'hidden.bs.modal', (e) ->
  info = getSourceInfo()
  # если окно закрылось при нажатии кнопки, то удалять ddl не надо
  info['delete_ddl'] = !dataWindow.data('load')
  $.get closeUrl, info, (res) ->
    if res.status == 'error'
      confirmAlert res.message
    return
  return
