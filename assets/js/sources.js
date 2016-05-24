// Generated by CoffeeScript 1.10.0
var addCol, addNewJoin, cancelRenameColumn, checkConnection, checkRightCheckboxes, checkTable, chosenTables, closeJoins, closeSettings, closeUrl, colsHeaders, colsTemplate, confirmAlert, createSettigns, dataWindow, dataWorkspace, delCol, deleteJoins, drawTables, getColumns, getConnectionData, getSourceInfo, hasWithoutBinds, initDataTable, insertJoinRows, joinWin, joinWinRow, loader, refreshData, removeSource, renameColumn, saveColumnName, saveJoins, saveNewSource, search, selectedRow, setActive, showJoinWindow, startLoading, tableToLeft, tableToRight, tablesToLeft, tablesToRight,
getSourceId;

chosenTables = colsTemplate = colsHeaders = joinWinRow = joinWin = selectedRow = dataWorkspace = loader = initDataTable = closeUrl = dataWindow = null;

confirmAlert = function(message) {
  $.confirm({
    width: '100px',
    text: message,
    title: 'Внимание!',
    confirmButtonClass: 'btn-danger',
    cancelButtonClass: 'hidden',
    confirmButton: 'Ок'
  });
};

checkConnection = function() {
  var form, formData, url;
  form = $('#conn_form');
  formData = new FormData(form[0]);
  url = form.attr('data-url');
  $.validator.messages.required = 'Обязательное поле!';
  form.validate({
    rules: {
      port: {
        number: true
      },
      password: {
        required: false
      }
    },
    messages: {
      port: {
        number: 'Введите целое число!'
      }
    }
  });
  $.each(form.find('.border-red'), function(i, el) {
    $(el).removeClass('border-red');
  });
  if (!form.valid()) {
    $.each(form.validate().errorList, function(i, el2) {
      $(el2.element).addClass('border-red');
    });
    return false;
  }
  $.ajax({
    url: url,
    data: formData,
    processData: false,
    contentType: false,
    type: 'POST',
    success: function(result) {
      if (result.status === 'error') {
        $.confirm({
          text: result.message || 'Подключение не удалось!',
          title: 'Внимание',
          confirmButtonClass: 'btn-danger',
          cancelButtonClass: 'hidden',
          confirmButton: 'Ок'
        });
      } else if (result.status === 'success') {
        $.confirm({
          width: '100px',
          text: result.message || 'Подключение удалось!',
          title: 'Внимание',
          cancelButtonClass: 'hidden',
          confirmButton: 'Ок'
        });
      }
    }
  });
};

search = function() {
  var search;
  var etlUrl;
  etlUrl = $('#source_table').attr('data-url');
  search = $('#search').val();
  document.location = etlUrl + '?search=' + search;
};

removeSource = function(url) {
  $.confirm({
    text: 'Вы действительно хотите удалить источник?',
    confirm: function(button) {
      $.post(url, {
        csrfmiddlewaretoken: csrftoken
      }, function(data) {
        window.location = data.redirect_url;
      });
    },
    title: 'Удаление источника',
    confirmButton: 'Удалить',
    cancelButton: 'Отмена'
  });
};

createSettigns = function() {
  $.validator.messages.required = 'Обязательное поле!';
  if (!$('#conn_form').valid()) {
    return;
  }
  $('#settings-window').modal('show');
};

saveNewSource = function(save_url) {
  var connection_form, formData, url;
  connection_form = $('#conn_form');
  formData = new FormData(connection_form[0]);
  url = save_url || connection_form.attr('data-save-url');
  formData.append('cdc_type', $('#cdc_select').val());
  $.validator.messages.required = 'Обязательное поле!';
  if (!connection_form.valid()) {
    return false;
  }
  $.ajax({
    url: url,
    data: formData,
    processData: false,
    contentType: false,
    type: 'POST',
    success: function(result) {
      $('#settings-window').modal('hide');
      if (result.status === 'error') {
        confirmAlert(result.message);
      } else {
        window.location = result.redirect_url;
      }
    }
  });
};

closeSettings = function() {
  $('#settings-window').modal('hide');
};

getConnectionData = function(dataUrl, closingUrl) {
  closeUrl = closingUrl;
  colsTemplate = _.template($('#table-cols').html());
  colsHeaders = _.template($('#cols-headers').html());
  selectedRow = _.template($('#selected-rows').html());
  initDataTable = _.template($('#datatable-init').html());
  joinWinRow = _.template($('#join-win-row').html());
  dataWindow = $('#modal-data');
  joinWin = $(_.template($('#join-window-modal').html())());
  loader = $('#loader');
  loader.hide();
  $.get(dataUrl, {
    csrfmiddlewaretoken: csrftoken
  }, function(res) {
    var rowsTemplate;
    rowsTemplate = _.template($('#database-rows').html());
    $('#databases').html(rowsTemplate({
      data: res.data
    }));
    chosenTables = $('#chosenTables');
    dataWorkspace = $('#data-workspace');
    chosenTables.html('');
    dataWorkspace.html(initDataTable);
    dataWindow.modal('show');
    $('#button-toRight').addClass('disabled');
    $('#button-allToRight').addClass('disabled');
    $('#button-toLeft').addClass('disabled');
    $('#button-allToLeft').addClass('disabled');
    if (res.status === 'error') {
      confirmAlert(res.message);
    }
  });
};

checkTable = function(table) {
  var checkboxes, tableRow;
  tableRow = $('#' + table);
  if (tableRow.hasClass('table-selected')) {
    tableRow.removeClass('table-selected');
    $('#button-toRight').addClass('disabled');
  }
  checkboxes = $('.checkbox-table:checked');
  if (checkboxes.length) {
    $('#button-allToRight').removeClass('disabled');
  } else {
    $('#button-allToRight').addClass('disabled');
  }
};

setActive = function(table) {
  var checkboxes, tableRow;
  tableRow = $('#' + table);
  if (tableRow.hasClass('table-selected')) {
    tableRow.removeClass('table-selected');
    $('.checkbox-table').prop('checked', false);
    $('#button-toRight').addClass('disabled');
    checkboxes = $('.checkbox-table:checked');
    if (!checkboxes.length) {
      $('#button-allToRight').addClass('disabled');
    }
  } else {
    $('.checkbox-table').prop('checked', false);
    $('.table-selected').removeClass('table-selected');
    tableRow.addClass('table-selected');
    tableRow.find('input[type="checkbox"]').prop('checked', true);
    $('#button-toRight').removeClass('disabled');
  }
};

checkRightCheckboxes = function() {
  if ($('.right-chbs:checked').length) {
    $('#button-toLeft').removeClass('disabled');
  } else {
    $('#button-toLeft').addClass('disabled');
  }
};

drawTables = function(data) {
  chosenTables.html('');
  if (data[0].is_root) {
    chosenTables.append(colsTemplate({
      row: data[0]
    }));
    data = data.slice(1);
  }
  _.each(data, function(el) {
    $('#for-' + el.dest + '-childs').append(colsTemplate({
      row: el
    }));
  });
};

getColumns = function(url, dict) {
  $.get(url, dict, function(res) {
    if (res.status === 'error') {
      confirmAlert(res.message);
    } else {
      drawTables(res.data);
      $('#data-table-headers').html('');
      $('#data-table-headers').append(colsHeaders({
        data: res.data
      }));
      $('#button-allToLeft').removeClass('disabled');
    }
  });
};

hasWithoutBinds = function() {
  if ($('.without_bind').length) {
    confirmAlert('Обнаружены ошибки в связях! ' + 'Выберите правильную связь у таблицы, либо удалите ее!');
    return true;
  }
  return false;
};

tableToRight = function(url) {
  var selectedTable;
  if ($('#button-toRight').hasClass('disabled')) {
    return;
  }
  if (hasWithoutBinds()) {
    return;
  }
  selectedTable = $('div.table-selected');
  if (selectedTable.length && !$('#' + selectedTable.attr('id') + 'Cols').length) {
    dataWorkspace.find('.result-col').remove();
    getColumns(url, {
      csrfmiddlewaretoken: csrftoken,
      sourceId: getSourceId(),
      tables: JSON.stringify([selectedTable.attr('data-table')])
    });
  }
};

tablesToRight = function(url) {
  var dict, divs, tables;
  if ($('#button-allToRight').hasClass('disabled')) {
    return;
  }
  if (hasWithoutBinds()) {
    return;
  }
  divs = $('.checkbox-table:checked').closest('div');
  dict = {
    csrfmiddlewaretoken: csrftoken,
    sourceId: getSourceId()
  };
  tables = divs.map(function() {
    var el, id;
    el = $(this);
    id = el.attr('id');
    if (!$('#' + id + 'Cols').length) {
      return el.attr('data-table');
    }
  }).get();
  if (tables.length) {
    dataWorkspace.find('.result-col').remove();
    dict['tables'] = JSON.stringify(tables);
    getColumns(url, dict);
  }
};

addCol = function(tName, colName) {
  var col, index, ths, workspaceRows;
  if (!$('#head-' + tName + '-' + colName + ':visible').length) {
    $('#for-head-' + tName + '-' + colName).css('font-weight', 'bold');
    col = $('#head-' + tName + '-' + colName);
    ths = $('#data-table-headers').find('th');
    index = ths.index(col);
    workspaceRows = dataWorkspace.find('table tr').not(':first');
    $(workspaceRows).each(function(trIndex, tRow) {
      if (!index) {
        $(tRow).prepend('<td></td>');
      } else {
        $('<td></td>').insertAfter($(tRow).find('td').eq(index - 1));
      }
    });
    col.show();
    col.addClass('data-table-column-header');
  }
};

delCol = function(id) {
  var header, index, ths, workspaceRows;
  if ($('#' + id + ':visible').length) {
    $('#for-' + id).css('font-weight', 'normal');
    $('#' + id).hide();
    $('#' + id).removeClass('data-table-column-header');
    ths = $('#data-table-headers').find('th');
    header = $('#' + id);
    index = ths.index(header);
    workspaceRows = dataWorkspace.find('table tr').not(':first');
    $(workspaceRows).each(function(trIndex, tRow) {
      $(tRow).find('td').eq(index).remove();
      if ($(tRow).length === 0) {
        $(tRow).remove();
      }
    });
  }
};

getSourceId = function() {
  var source = $('#databases>div');
  return source.data('source')
};

getSourceInfo = function() {
  return {
    sourceId: getSourceId()
  };
};


tableToLeft = function(url) {
  var checked, checked2, divs, indexes, info, reversed, selTables, selTables2, tablesToDelete, ths, workspaceRows;
  if ($('#button-toLeft').hasClass('disabled')) {
    return;
  }
  checked = $('.right-chbs:checked').closest('.table-part').find('.right-chbs');
  divs = checked.siblings('div').find('div');
  indexes = [];
  ths = $('#data-table-headers').find('th').not(':hidden');
  $.each(divs, function(i, el) {
    var header;
    header = $('#head-' + $(this).data('table') + '-' + $(this).data('col'));
    indexes.push(ths.index(header));
    header.remove();
  });
  workspaceRows = dataWorkspace.find('table tr').not(':first');
  reversed = indexes.reverse();
  workspaceRows.remove();
  selTables = checked.closest('.table-part');
  tablesToDelete = [];
  $.each(selTables, function(i, el) {
    $(this).closest('.table-part').find('.table-part').remove();
  });
  checked2 = $('.right-chbs:checked');
  selTables2 = checked2.closest('.table-part');
  $.each(selTables2, function(i, el) {
    tablesToDelete.push($(this).data('table'));
    $(this).remove();
  });
  info = getSourceInfo();
  info['tables'] = JSON.stringify(tablesToDelete);
  $.get(url, info, function(res) {
    if (res.status === 'error') {
      confirmAlert(res.message);
    }
  });
  checkRightCheckboxes();
  if (!chosenTables.children().length) {
    $('#button-allToLeft').addClass('disabled');
  }
};

tablesToLeft = function(url) {
  var info;
  if ($('#button-allToLeft').hasClass('disabled')) {
    return;
  }
  info = getSourceInfo();
  info['delete_ddl'] = true;
  $.get(url, info, function(res) {
    if (res.status === 'error') {
      confirmAlert(res.message);
    } else {
      chosenTables.html('');
      dataWorkspace.html(initDataTable);
      $('#button-toLeft').addClass('disabled');
      $('#button-allToLeft').addClass('disabled');
    }
  });
};

refreshData = function(url) {
  var array, cols, colsInfo, source;
  if (hasWithoutBinds()) {
    return;
  }
  source = $('#databases>div');
  colsInfo = {
    sourceId: getSourceId()
  };
  cols = dataWorkspace.find('.data-table-column-header');
  array = cols.map(function() {
    var el;
    el = $(this);
    return {
      'table': el.data('table'),
      'col': el.data('col')
    };
  }).get();
  if (array.length) {
    colsInfo['cols'] = JSON.stringify(array);
    dataWorkspace.find('table tr').not(':first').remove();
    loader.show();
    dataWorkspace.parent('div').css('background-color', '#ddd');
    $.post(url, colsInfo, function(res) {
      var tableData;
      if (res.status === 'error') {
        confirmAlert(res.message);
      } else {
        tableData = dataWorkspace.find('table > tbody');
        tableData.append(selectedRow({
          data: res.data
        }));
      }
      loader.hide();
      dataWorkspace.parent('div').css('background-color', 'white');
    });
  }
};

insertJoinRows = function(data, parent, child, joinRows) {
  var goodLen;
  $.each(data.good_joins, function(i, join) {
    var newRow;
    newRow = joinWinRow({
      parentCols: data.columns[parent],
      childCols: data.columns[child],
      i: i,
      error: false
    });
    joinRows.append($(newRow));
    $('[name="joinradio"][value=' + join['join']['type'] + ']').prop('checked', true);
    $('.with-select-' + i).find('select[name="parent"]').val(join['left']['column']);
    $('.with-select-' + i).find('select[name="child"]').val(join['right']['column']);
    $('.with-select-' + i).find('select[name="joinType"]').val(join['join']['value']);
  });
  goodLen = data.good_joins.length;
  $.each(data.error_joins, function(i, join) {
    var j, newRow;
    j = i + goodLen;
    newRow = joinWinRow({
      parentCols: data.columns[parent],
      childCols: data.columns[child],
      i: j,
      error: true
    });
    joinRows.append($(newRow));
    $('[name="joinradio"][value=' + join['join']['type'] + ']').prop('checked', true);
    $('.with-select-' + j).find('select[name="parent"]').val(join['left']['column']);
    $('.with-select-' + j).find('select[name="child"]').val(join['right']['column']);
    $('.with-select-' + j).find('select[name="joinType"]').val(join['join']['value']);
  });
};

showJoinWindow = function(url, parent, child, isWithoutBind) {
  var info, warn;
  info = getSourceInfo();
  info['parent'] = parent;
  info['child_bind'] = child;
  warn = $('#table-part-' + child + '>div:first').find('.without_bind');
  info['has_warning'] = warn.length ? true : false;
  $.get(url, info, function(res) {
    var data, joinRows;
    if (res.status === 'error') {
      confirmAlert(res.message);
    } else {
      joinRows = joinWin.find('#joinRows');
      data = res.data;
      joinRows.html('');
      joinRows.data('table-left', parent);
      joinRows.data('table-right', child);
      if (!data.good_joins.length && !data.error_joins.length) {
        joinRows.append(joinWinRow({
          parentCols: data.columns[parent],
          childCols: data.columns[child],
          i: 0,
          error: false
        }));
      } else {
        insertJoinRows(data, parent, child, joinRows);
      }
      joinWin.find('#parentLabel').text(parent);
      joinWin.find('#childLabel').text(child);
      joinWin.modal('show');
    }
  });
};

addNewJoin = function() {
  var childCols, joinRows, parOptions, parentCols, wobOptions;
  joinRows = joinWin.find('#joinRows');
  parentCols = [];
  childCols = [];
  parOptions = joinWin.find('select[name="parent"]').first().find('option');
  wobOptions = joinWin.find('select[name="child"]').first().find('option');
  $.each(parOptions, function(i, el) {
    parentCols.push($(el).attr('value'));
  });
  $.each(wobOptions, function(i, el) {
    childCols.push($(el).attr('value'));
  });
  joinRows.append(joinWinRow({
    parentCols: parentCols,
    childCols: childCols,
    i: 0,
    error: false
  }));
};

deleteJoins = function() {
  $('.checkbox-joins:checked').closest('.join-row').remove();
};

saveJoins = function(url) {
  var info, joinRows, joins, joinsArray, joinsSet;
  joins = joinWin.find('.join-row');
  joinsArray = [];
  if (!joins.length) {
    confirmAlert('Пожалуйста, выберите связь!');
    return;
  }
  $.each(joins, function(i, row) {
    var selects, vals;
    selects = $(row).find('select');
    vals = [];
    $.each(selects, function(j, sel) {
      vals.push($(sel).val());
    });
    joinsArray.push(vals);
  });
  joinsSet = new Set;
  $.each(joinsArray, function(i, row) {
    joinsSet.add(row[0] + row[2]);
  });
  if (joinsArray.length !== joinsSet.size) {
    confirmAlert('Имеются дубли среди связей, пожалуйста удалите лишнее!');
    return;
  }
  joinRows = joinWin.find('#joinRows');
  info = getSourceInfo();
  info['joins'] = JSON.stringify(joinsArray);
  info['left'] = joinRows.data('table-left');
  info['right'] = joinRows.data('table-right');
  info['joinType'] = $('[name="joinradio"]:checked').val();
  $.get(url, info, function(res) {
    var rel, rightTableArea, warn;
    if (res.status === 'error') {
      confirmAlert(res.message);
    } else {
      joinWin.modal('hide');
      rightTableArea = $('#table-part-' + joinRows.data('table-right') + '>div:first');
      rel = rightTableArea.find('.relation');
      warn = $('<span class="without_bind" style="color:red;">!!!</span>');
      if (res.data.has_error_joins === true) {
        if (!rel.find('.without_bind').length) {
          rel.append(warn);
        }
      } else {
        rightTableArea.find('.without_bind').remove();
      }
      if (!$('.without_bind').length) {
        drawTables(res.data.draw_table);
      }
    }
  });
};

closeJoins = function() {
  joinWin.modal('hide');
};

startLoading = function(userId, loadUrl) {
  var array, cols, info, tables, tablesArray;
  info = getSourceInfo();
  tables = new Set;
  cols = dataWorkspace.find('.data-table-column-header');
  array = cols.map(function() {
    var el;
    el = $(this);
    tables.add(el.data('table'));
    return {
      'table': el.data('table'),
      'col': el.data('col')
    };
  }).get();
  if (hasWithoutBinds()) {
    return;
  }
  if (!array.length) {
    confirmAlert('Выберите таблицы для загрузки!');
    return;
  }
  tablesArray = [];
  tables.forEach(function(el) {
    tablesArray.push(el);
  });
  info['cols'] = JSON.stringify(array);
  info['tables'] = JSON.stringify(tablesArray);
  $.post(loadUrl, info, function(response) {
    var channels, tasksUl;
    if (response.status === 'error') {
      confirmAlert(response.message);
    } else {
//      dataWindow.data('load', true);
//      dataWindow.modal('hide');
//      dataWindow.data('load', false);
//      channels = response.data['channels'];
//      tasksUl = $('#user_tasks_bar');
//      _.each(channels, function(channel) {
//        var q;
//        q = new Queue2(tasksUl.data('host'), tasksUl.data('port'), '/ws');
//        q.subscribe(channel);
//      });
    }
  });
};

renameColumn = function(headerId) {
  $('#text-' + headerId).hide();
  $('#cancel-' + headerId).show();
  $('#input-' + headerId).show();
};

cancelRenameColumn = function(headerId) {
  $('#text-' + headerId).show();
  $('#input-' + headerId).hide();
  $('#cancel-' + headerId).hide();
};

saveColumnName = function(headerId, event, url) {
  var cancel, head, info, input, newColumnName, realColumnName, table, text;
  if (event.keyCode === 13) {
    head = $('#' + headerId);
    text = $('#text-' + headerId);
    input = $('#input-' + headerId);
    cancel = $('#cancel-' + headerId);
    table = head.data('table');
    realColumnName = head.data('col');
    newColumnName = input.val();
    text.text(newColumnName);
    text.show();
    input.hide();
    cancel.hide();
    info = getSourceInfo();
    info['table'] = table;
    info['column'] = realColumnName;
    info['title'] = newColumnName;
    $.post(url, info, function(response) {});
  }
};

$('#modal-data').on('hidden.bs.modal', function(e) {
  var info;
  info = getSourceInfo();
  info['delete_ddl'] = !dataWindow.data('load');
  $.get(closeUrl, info, function(res) {
    if (res.status === 'error') {
      confirmAlert(res.message);
    }
  });
});

//# sourceMappingURL=sources.js.map
