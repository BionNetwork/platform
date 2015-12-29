
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

function validateSourceForm(f){
    $.validator.messages.required = 'Обязательное поле!';
    f.validate({
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
        },
    });
    $.each(f.find('.border-red'), function(i, el){
        $(el).removeClass('border-red');
    });

    if(!f.valid()){
        $.each(f.validate().errorList, function(i, el2){
            $(el2.element).addClass('border-red');
        })
        return false
    }
    return true
}

function checkConnection(){
    var form = $('#conn_form'),
        formData = new FormData(form[0]),
        url = form.attr('data-url');

    if(!validateSourceForm(form)){
        return;
    }

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


function createSettigns(){
    var form = $('#conn_form');
    if(!validateSourceForm(form)){
        return;
    }
    $('#settings-window').modal('show');
}

function saveNewSource(){
    var form = $('#conn_form'),
        formData = new FormData(form[0]);
        url = form.attr('data-save-url');
        formData.append('cdc_type', $('#cdc_select').val());

    $.ajax({
        url: url,
        data: formData,
        processData: false,
        contentType: false,
        type: 'POST',
        success: function(result){
            $('#settings-window').modal('hide');

            if (result.status=='error'){
                confirmAlert(result.message);
            } else {
                window.location = result.redirect_url;
            }
        }
    });
}

function closeSettings(){
    $('#settings-window').modal('hide');
}


var chosenTables, colsTemplate, colsHeaders, joinWinRow, joinWin,
    selectedRow, dataWorkspace, loader, initDataTable, closeUrl,
    dataWindow;

// событие на закрытие модального окна
$('#modal-data').on('hidden.bs.modal', function(e){
    var info = getSourceInfo();
    // если окно закрылось при нажатии кнопки, то удалять ddl не надо
    info['delete_ddl'] = !dataWindow.data('load');
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
    joinWin = $('#join-window');

    loader = $('#loader');
    loader.hide();

    $.get(dataUrl,
        {csrfmiddlewaretoken: csrftoken},
        function(res){

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


function hasWithoutBinds(){
    // если есть талица без связи, то внимание
    if($('.without_bind').length){
        confirmAlert('Обнаружены ошибки в связях! '+
        'Выберите правильную связь у таблицы, либо удалите ее!');
        return true;
    }
    return false;
}


function tableToRight(url){

    if($('#button-toRight').hasClass('disabled')){
        return;
    }

    if(hasWithoutBinds()){
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

    if(hasWithoutBinds()){
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
    var source = $('#databases>div');
    return {
        "host": source.data("host"),
        "db": source.data("db")
    };
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
    // удалять ddl надо
    info['delete_ddl'] = true;

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

    if(hasWithoutBinds()){
        return;
    }

    var source = $('#databases>div'),
        colsInfo = {
            "host": source.data("host"),
            "db": source.data("db")
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

        $.post(url, colsInfo, function(res){
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

function insertJoinRows(data, parent, child, joinRows){

    $.each(data.good_joins, function(i, join){
        var newRow = joinWinRow({
            parentCols: data.columns[parent],
            childCols: data.columns[child],
            i: i,
            error: false
        });
        joinRows.append($(newRow));
        $('[name="joinradio"][value='+join['join']['type']+']').prop('checked', true);

        $('.with-select-'+i).find('select[name="parent"]').val(join['left']['column']);
        $('.with-select-'+i).find('select[name="child"]').val(join['right']['column']);
        $('.with-select-'+i).find('select[name="joinType"]').val(join['join']['value']);
    });

    var goodLen = data.good_joins.length;

    $.each(data.error_joins, function(i, join){

        var j = i + goodLen;

        var newRow = joinWinRow({
            parentCols: data.columns[parent],
            childCols: data.columns[child],
            i: j,
            error: true
        });
        joinRows.append($(newRow));
        $('[name="joinradio"][value='+join['join']['type']+']').prop('checked', true);

        $('.with-select-'+j).find('select[name="parent"]').val(join['left']['column']);
        $('.with-select-'+j).find('select[name="child"]').val(join['right']['column']);
        $('.with-select-'+j).find('select[name="joinType"]').val(join['join']['value']);
    });
}

function showJoinWindow(url, parent, child, isWithoutBind){

    var info = getSourceInfo();
    info['parent'] = parent;
    info['child_bind'] = child;

    var warn = $('#table-part-'+child+'>div:first').find('.without_bind');
    info['has_warning'] = warn.length ? true : false;

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

            // последняя таблица без связей
            if(!data.good_joins.length && !data.error_joins.length){
                joinRows.append(joinWinRow({
                    parentCols: data.columns[parent],
                    childCols: data.columns[child],
                    i: 0,
                    error: false
                }));
            }
            else {
                insertJoinRows(data, parent, child, joinRows)
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
        i: 0,
        error: false
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

    var joinsSet = new Set();
    // избавляемся от дублей джойнов
    $.each(joinsArray, function(i, row){
        joinsSet.add(row[0]+row[2]);
    });

    if(joinsArray.length != joinsSet.size){
        confirmAlert('Имеются дубли среди связей, пожалуйста удалите лишнее!');
        return;
    }

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

            var rightTableArea = $('#table-part-'+joinRows.data('table-right')+'>div:first'),
                   rel = rightTableArea.find('.relation'),
                   warn = $('<span class="without_bind" style="color:red;">!!!</span>');

            // если новые джойны неверны, добавляем красное, если еще не было
            if(res.data.has_error_joins == true){
                if(!rel.find('.without_bind').length){
                    rel.append(warn);
                }
            }
            // если новые джойны верны, удаляем красное
            else { // res.data.has_error_joins == false
                rightTableArea.find('.without_bind').remove();
            }

            // если совсем нет ошибок ни у кого, то перерисуем дерево,
            // на всякий пожарный
            if(!$('.without_bind').length){
                drawTables(res.data.draw_table);
            }
        }
    });
}


function closeJoins(){
    joinWin.modal('hide');
}


function startLoading(userId, loadUrl){
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

    if(hasWithoutBinds()){
        return;
    }

    if(!array.length){
        confirmAlert("Выберите таблицы для загрузки!");
        return
    }

    var tablesArray = [];
    tables.forEach(function(el){
        tablesArray.push(el);
    });

    info['cols'] = JSON.stringify(array);
    info['tables'] = JSON.stringify(tablesArray);

    $.post(loadUrl, info, function(response){
        if(response.status == 'error') {
                confirmAlert(response.message);
        } else {
            // признак того, что окно закрылось при нажатии кнопки
            dataWindow.data('load', true);
            dataWindow.modal('hide');// clear data
            dataWindow.data('load', false);

            var channels = response.data['channels'],
                tasksUl = $('#user_tasks_bar');

//            _.each(channels, function(channel){
//                var q = new Queue2(
//                            tasksUl.data('host'), tasksUl.data('port'), '/ws');
//
//                // подписка на канал
//                q.subscribe(channel);
//            });
        }
    });
}
