
function p(m){
    console.log(m);
}

function confirmAlert(message){
    $.confirm({
        width: '100px',
        text: message,
        title:"Внимание",
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

var chosenTables, colsTemplate, colsHeaders,
    selectedRow, headers, loader;

function getConnectionData(dataUrl){

    colsTemplate = _.template($('#table-cols').html());
    colsHeaders = _.template($('#cols-headers').html());
    selectedRow = _.template($('#selected-rows').html());

    loader = $('#loader');
    loader.hide();

    $.get(dataUrl,
        {csrfmiddlewaretoken: csrftoken},
        function(res){
            _.each(res.data.tables,
                function(el){el['display'] = el['name'].substr(0, 23);});

            var rowsTemplate = _.template($('#database-rows').html()),
                dataWindow = $('#modal-data');
            $('#databases').html(rowsTemplate({data: res.data}));

            chosenTables = $('#chosenTables');

            headers = $('#headers');

            chosenTables.html('');
            headers.html('');

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

function getColumns(url, dict){
    $.get(url, dict,
        function(res){
            if(res.status == 'error'){
               confirmAlert(res.message);
            }
            else{
                chosenTables.append(colsTemplate({data: res.data}));

                if($('#headers').length){
                    $('#headers').append(colsHeaders({data: res.data}));
                }
                else{
                    headers.append(colsHeaders({data: res.data}));
                }

                $('#button-toLeft').removeClass('disabled');
                $('#button-allToLeft').removeClass('disabled');
            }
        }
    );
}

function tableToRight(url){
    var selectedTable = $('div.table-selected');

    if(selectedTable.length && !$('#'+selectedTable.attr('id')+'Cols').length){

        headers.find('.result-col').remove();

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

        headers.find('.result-col').remove();

        dict['tables'] = JSON.stringify(tables);
        getColumns(url, dict);
    }
}

function addCol(tName, colName){
    $('#for-col-'+tName+'-'+colName).css('font-weight', 'bold');

    var col = $('#col-'+tName+'-'+colName);

    if(!col.length){
        headers.append(
            colsHeaders({data: [{tname: tName, cols: [colName]}]}));
    }
    else{
        col.show();
        col.addClass("select-col-div");
    }
}

function delCol(id){
    $('#for-'+id).css('font-weight', 'normal');
    $('#'+id).hide();
    $('#'+id).removeClass("select-col-div");
}

function tableToLeft(){
    var checked = $('.right-chbs:checked'),
        divs = checked.siblings('div').find('div');
    $.each(divs, function(i, el){
        $('#col-'+$(this).data('table')+'-'+$(this).data('col')).remove();
    });
    checked.closest('div').remove();

    if(!chosenTables.children().length){
        $('#button-toLeft').addClass('disabled');
        $('#button-allToLeft').addClass('disabled');
    }
}

function tablesToLeft(){
    chosenTables.html('');
    headers.html('');
    $('#button-toLeft').addClass('disabled');
    $('#button-allToLeft').addClass('disabled');
}

function refreshData(url){

    var source = $('#databases>div'),
        colsInfo = {
            "host": source.data("host"),
            "db": source.data("db"),
        },
        cols = headers.find('.select-col-div'),
        array = cols.map(function(){
            var el = $(this);
            return {
                "table": el.data("table"),
                "col": el.data("col")
            }
        }).get();

    if(array.length){
        colsInfo['cols'] = JSON.stringify(array);

        headers.html('');

        loader.show();
        headers.parent('div').css('background-color', '#ddd');

        $.get(url, colsInfo, function(res){
            if (res.status == 'error') {
              confirmAlert(res.message)
            } else {
                headers.append(selectedRow({data: res.data}));
                loader.hide();
                headers.parent('div').css('background-color', 'white');
            }
        });
    }
}
