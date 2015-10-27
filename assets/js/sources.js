
function confirmAlert(message){
    $.confirm({
        width: '100px',
        text: message,
        title:"Внимание!",
        confirmButtonClass: "btn-danger",
        cancelButtonClass: "hidden",
        confirmButton: "Ок"
    });
}

function checkConnection(){
    var form = $('#conn_form'),
        formData = new FormData(form[0]),
        url = form.attr('data-url');

    $.validator.messages.required = 'Обязательное поле!';

    form.valid();

    $.ajax({
        url: url,
        data: formData,
        processData: false,
        contentType: false,
        type: 'POST',
        success: function(result){
            if(result.status == 'error'){
                $.confirm({
                    text: result.message || "Подключение не удалось!",
                    title:"Внимание",
                    confirmButtonClass: "btn-danger",
                    cancelButtonClass: "hidden",
                    confirmButton: "Ок"
                });
            }
            else if(result.status == 'success'){
                $.confirm({
                    width: '100px',
                    text: result.message || "Подключение удалось!",
                    title:"Внимание",
                    cancelButtonClass: "hidden",
                    confirmButton: "Ок"
                });
            }
        }
    });
}

function search(){
    var etlUrl = $('#source_table').attr('data-url'),
        search = $('#search').val();
    document.location = etlUrl+'?search='+search;
}

function removeSource(url){
    $.confirm({
        text: "Вы действительно хотите удалить источник?",
        confirm: function(button) {
            $.post(url,
                {csrfmiddlewaretoken: csrftoken},
                function(data) {
                    window.location = data.redirect_url;
                }
            );
        },
        title:"Удаление источника",
        confirmButton: "Удалить",
        cancelButton: "Отмена"
    });
}

var chosenTables, colsTemplate, colsHeaders, joinWinRow, joinWin,
    selectedRow, dataWorkspace, loader, initDataTable, closeUrl,
    dataWindow;

// событие на закрытие модального окна
$('#modal-data').on('hidden.bs.modal', function(e){
    var info = getSourceInfo();
    $.get(closeUrl, info, function(res){
        if (res.status == 'error'){
            confirmAlert(res.message);
        }
    });
});

function getConnectionData(dataUrl, closingUrl){
    closeUrl = closingUrl;
    colsTemplate = _.template($('#table-cols').html());
    colsHeaders = _.template($('#cols-headers').html());
    selectedRow = _.template($('#selected-rows').html());
    initDataTable = _.template($("#datatable-init").html());
    joinWinRow = _.template($("#join-win-row").html());

    dataWindow = $('#modal-data');
    joinWin = $('#join-window')

    loader = $('#loader');
    loader.hide();

    $.get(dataUrl,
        {csrfmiddlewaretoken: csrftoken},
        function(res){
            _.each(res.data.tables,
                function(el){el['display'] = el['name'].substr(0, 21);});

            var rowsTemplate = _.template($('#database-rows').html());

            $('#databases').html(rowsTemplate({data: res.data}));

            chosenTables = $('#chosenTables');

            dataWorkspace = $('#data-workspace');

            chosenTables.html('');
            dataWorkspace.html(initDataTable);

            dataWindow.modal('show');

            $('#button-toRight').addClass('disabled');
            $('#button-allToRight').addClass('disabled');
            $('#button-toLeft').addClass('disabled');
            $('#button-allToLeft').addClass('disabled');

            if(res.status == 'error'){
                confirmAlert(res.message);
            }
        }
    );
}

function checkTable(table) {
    var tableRow = $('#' + table);
    if (tableRow.hasClass('table-selected')) {
        tableRow.removeClass('table-selected');
        $('#button-toRight').addClass('disabled');
    }

    var checkboxes = $('.checkbox-table:checked');
    if (checkboxes.length) {
        $('#button-allToRight').removeClass('disabled');
    }
    else {
        $('#button-allToRight').addClass('disabled');
    }
}

function setActive(table) {
    var tableRow = $('#' + table);
    if (tableRow.hasClass('table-selected')) {
        tableRow.removeClass('table-selected');
        $('.checkbox-table').prop('checked', false);
        $('#button-toRight').addClass('disabled');

        var checkboxes = $('.checkbox-table:checked');
        if (!checkboxes.length) {
            $('#button-allToRight').addClass('disabled');
        }
    }
    else {
        $('.checkbox-table').prop('checked', false);
        $(".table-selected").removeClass("table-selected");
        tableRow.addClass('table-selected');
        tableRow.find('input[type="checkbox"]').prop('checked', true);
        $('#button-toRight').removeClass('disabled');
    }
}

function checkRightCheckboxes(){
    if($('.right-chbs:checked').length){
        $('#button-toLeft').removeClass('disabled');
    } else {
        $('#button-toLeft').addClass('disabled');
    }
}


function drawTables(data){

    chosenTables.html('');

    if(data[0].is_root){
        chosenTables.append(colsTemplate({row: data[0]}));
        data = data.slice(1);
    }
    _.each(data,
        function(el){
            $('#for-'+el.dest+'-childs').append(colsTemplate({row: el}))
        });
}


function getColumns(url, dict) {
    $.get(url, dict,
        function (res) {
            if (res.status == 'error') {
                confirmAlert(res.message);
            } else {
                drawTables(res.data);
                $('#data-table-headers').html('');
                $('#data-table-headers').append(colsHeaders({data: res.data}));
                $('#button-allToLeft').removeClass('disabled');
            }
        }
    );
}

function tableToRight(url){

    if($('#button-toRight').hasClass('disabled')){
        return;
    }

    // если есть талица без связи, то внимание
    if($('#without_bind').length){
        confirmAlert('Имеется таблица без связи! '+
        'Выберите связь у таблицы, либо удалите ее!');
        return;
    }

    var selectedTable = $('div.table-selected');

    if(selectedTable.length && !$('#'+selectedTable.attr('id')+'Cols').length){

        dataWorkspace.find('.result-col').remove();

        getColumns(url, {
                    csrfmiddlewaretoken: csrftoken,
                    host: selectedTable.attr('data-host'),
                    db : selectedTable.attr('data-db'),
                    tables: JSON.stringify([selectedTable.attr('data-table'), ])
                }
        );
    }
}

function tablesToRight(url){

    if($('#button-allToRight').hasClass('disabled')){
        return;
    }

    // если есть талица без связи, то внимание
    if($('#without_bind').length){
        confirmAlert('Имеется таблица без связи! '+
        'Выберите связь у таблицы, либо удалите ее!');
        return;
    }

    var divs = $('.checkbox-table:checked').closest('div'),
        dict = {
                csrfmiddlewaretoken: csrftoken,
                host: divs.attr('data-host'),
                db : divs.attr('data-db'),
            }

    var tables = divs.map(function(){
        var el = $(this),
            id = el.attr('id');
        if(!$('#'+id+'Cols').length){
            return el.attr('data-table');
        }
    }).get();

    if(tables.length){

        dataWorkspace.find('.result-col').remove();

        dict['tables'] = JSON.stringify(tables);
        getColumns(url, dict);
    }
}

function addCol(tName, colName){

    if(!$('#col-'+tName+'-'+colName+':visible').length){
        $('#for-col-'+tName+'-'+colName).css('font-weight', 'bold');
        var col = $('#col-'+tName+'-'+colName),
            ths = $("#data-table-headers").find("th"),
            index = ths.index(col),
            workspaceRows = dataWorkspace.find("table tr").not(":first");

        $(workspaceRows).each(function(trIndex, tRow){
            if (!index) {
                $(tRow).prepend('<td></td>');
            }
            else{
                $('<td></td>').insertAfter($(tRow).find('td').eq(index-1));
            }
        });
        col.show();
        col.addClass("data-table-column-header");
    }
}

function delCol(id){

    if($('#'+id+':visible').length){
        $('#for-'+id).css('font-weight', 'normal');
        $('#'+id).hide();
        $('#'+id).removeClass("data-table-column-header");

        var ths = $("#data-table-headers").find("th"),
            header = $('#'+id),
            index = ths.index(header),
            workspaceRows = dataWorkspace.find("table tr").not(":first");

        $(workspaceRows).each(function(trIndex, tRow){
            $(tRow).find("td").eq(index).remove();
            if ($(tRow).length == 0) {
                $(tRow).remove();
            }
        });
    }
}


function getSourceInfo(){
    var source = $('#databases>div'),
        info = {
            "host": source.data("host"),
            "db": source.data("db")
        };
    return info;
}


function tableToLeft(url){

    if($('#button-toLeft').hasClass('disabled')){
        return;
    }

    // чекбоксы с дочерними чекбоксами
    var checked = $('.right-chbs:checked').closest('.table-part').find('.right-chbs'),
        divs = checked.siblings('div').find('div'),
        indexes = [],// индексы в таблице для удаления
        ths = $("#data-table-headers").find("th").not(':hidden');

    $.each(divs, function(i, el){
        var header = $('#col-'+$(this).data('table')+'-'+$(this).data('col'));
        indexes.push(ths.index(header));
        header.remove();
    });

    var workspaceRows = dataWorkspace.find("table tr").not(":first"),
        reversed = indexes.reverse();

    // удаляем все строки данных
    workspaceRows.remove();

    // удаляем ячейки по индексам (функция работает некорректно)
//    $(workspaceRows).each(function(trIndex, tRow){
//        $.each(reversed, function(i, el){
//            $(tRow).find("td").eq(el).remove();
//        });
//        if ($(tRow).length == 0) {
//            $(tRow).remove();
//        }
//    });

    var selTables = checked.closest('.table-part'),
        tablesToDelete = [];

    $.each(selTables, function(i, el){
        $(this).closest('.table-part').find('.table-part').remove();
    });
    var checked2 = $('.right-chbs:checked'),
        selTables2 = checked2.closest('.table-part');

    $.each(selTables2, function(i, el){
       tablesToDelete.push($(this).data('table'));
       $(this).remove();
    });

    var info = getSourceInfo();
    info['tables'] = JSON.stringify(tablesToDelete);

    $.get(url, info, function(res){
        if (res.status == 'error') {
            confirmAlert(res.message);
        }
    });

    checkRightCheckboxes();

    if(!chosenTables.children().length){
        $('#button-allToLeft').addClass('disabled');
    }
}


function tablesToLeft(url){

    if($('#button-allToLeft').hasClass('disabled')){
        return;
    }

    var info = getSourceInfo();

    $.get(url, info, function(res){
        if (res.status == 'error') {
            confirmAlert(res.message);
        }
        else{
            chosenTables.html('');
            dataWorkspace.html(initDataTable);
            $('#button-toLeft').addClass('disabled');
            $('#button-allToLeft').addClass('disabled');
        }
    });
}

function refreshData(url){

    var source = $('#databases>div'),
        colsInfo = {
            "host": source.data("host"),
            "db": source.data("db"),
        },
        cols = dataWorkspace.find('.data-table-column-header'),
        array = cols.map(function(){
            var el = $(this);
            return {
                "table": el.data("table"),
                "col": el.data("col")
            }
        }).get();

    if(array.length) {
        colsInfo['cols'] = JSON.stringify(array);

        // удаляем все ячейки с данными
        dataWorkspace.find("table tr").not(":first").remove();

        loader.show();
        dataWorkspace.parent('div').css('background-color', '#ddd');

        $.get(url, colsInfo, function(res){
            if(res.status == 'error') {
                confirmAlert(res.message)
            } else {
                var tableData = dataWorkspace.find("table > tbody");
                tableData.append(selectedRow({data: res.data}));
            }
            loader.hide();
            dataWorkspace.parent('div').css('background-color', 'white');
        });
    }
}


function showJoinWindow(url, parent, child, isWithoutBind){

    var info = getSourceInfo();
    info['parent'] = parent;
    info['child_bind'] = child;
    info['is_without_bind'] = isWithoutBind;

    $.get(url, info, function(res){
        if (res.status == 'error') {
            confirmAlert(res.message);
        }
        else{
            var joinRows = $('#joinRows'),
                data = res.data;
            joinRows.html('');
            joinRows.data('table-left', parent);
            joinRows.data('table-right', child);

            if(data['without_bind']){
                joinRows.append(joinWinRow({
                    parentCols: data[parent],
                    childCols: data[child],
                    i: 0
                }));
            } else {
                $.each(data.joins, function(i, join){
                    var newRow = joinWinRow({
                        parentCols: data[parent],
                        childCols: data[child],
                        i: i
                    });
                    joinRows.append($(newRow));
                    $('[name="joinradio"][value='+join['join']['type']+']').prop('checked');

                    $('.with-select-'+i).find('select[name="parent"]').val(join['left']['column']);
                    $('.with-select-'+i).find('select[name="child"]').val(join['right']['column']);
                    $('.with-select-'+i).find('select[name="joinType"]').val(join['join']['value']);
                });
            }

            $('#parentLabel').text(parent);
            $('#childLabel').text(child);
            joinWin.modal('show');
        }
    });
}


function addNewJoin(){
    var joinRows = $('#joinRows'),
        parentCols = [],
        childCols = [],
        parOptions = joinWin.find('select[name="parent"]').first().find('option');
        wobOptions = joinWin.find('select[name="child"]').first().find('option');

    $.each(parOptions, function(i, el){
       parentCols.push($(el).attr('value'));
    });

    $.each(wobOptions, function(i, el){
       childCols.push($(el).attr('value'));
    });

    joinRows.append(joinWinRow({
        parentCols: parentCols,
        childCols: childCols,
        i: 0
    }));
}


function deleteJoins(){
    $('.checkbox-joins:checked').closest('.join-row').remove();
}


function saveJoins(url){

    var joins = $('.join-row'),
        joinsArray = [];

    if(!joins.length){
        confirmAlert('Пожалуйста, выберите связь!');
        return;
    }

    $.each(joins, function(i, row){
        var selects = $(row).find('select'),
            vals = [];
        $.each(selects, function(j, sel){
            vals.push($(sel).val());
        });
        joinsArray.push(vals);
    });

    var joinRows = $('#joinRows'),
        info = getSourceInfo();

    info['joins'] = JSON.stringify(joinsArray);
    info['left'] = joinRows.data('table-left');
    info['right'] = joinRows.data('table-right');
    info['joinType'] = $('[name="joinradio"]:checked').val();

    $.get(url, info, function(res){
        if(res.status == 'error') {
                confirmAlert(res.message)
        } else {
            joinWin.modal('hide');
            drawTables(res.data);
        }
    });
}


function closeJoins(){
    joinWin.modal('hide');
}


function startLoading(userId, taskNumUrl, loadUrl){

    $.get(taskNumUrl, {}, function(res){
        if(res.status == 'error') {
                confirmAlert(res.message)
        } else {
            var tasksUl = $('#user_tasks_bar'),
                ws = new WebSocket(
                "ws://"+tasksUl.data('host')+"user/"+userId+"/task/"+res.task_id);

            ws.onopen = function(){
                var taskTmpl = _.template($('#tasks_progress').html());
                tasksUl.append(taskTmpl({data: [res.task_id, ]}));
            };
            ws.onmessage = function (evt){
                $('#task-text-'+res.task_id).text(evt.data+'%')
                $('#task-measure-'+res.task_id).css('width', evt.data+'%')
            };
            ws.onclose = function(){
            };

            var info = getSourceInfo(),
                tables = new Set(),
                cols = dataWorkspace.find('.data-table-column-header'),
                array = cols.map(function(){
                    var el = $(this);
                    tables.add(el.data("table"));
                    return {
                        "table": el.data("table"),
                        "col": el.data("col")
                    }
                }).get();

            if(!array.length){
                confirmAlert("Выберите таблицы для загрузки!");
                return
            }

            info['task_id'] = res.task_id;
            info['cols'] = JSON.stringify(array);
            info['tables'] = JSON.stringify(Array.from(tables));

            $.get(loadUrl, info, function(res){
                if(res.status == 'error') {
                        confirmAlert(res.message);
                } else {
                    dataWindow.modal('hide');
                }
            });
        }
    });
}