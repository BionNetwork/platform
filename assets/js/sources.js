
//function p(m){
//    console.log(m);
//}

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

var chosenTables, chosenColsRows, colsTemplate, colNames,
    selectedRow;

function getConnectionData(dataUrl){

    colsTemplate = _.template($('#table-cols').html());
    colsNames = _.template($('#cols-names').html());
    selectedRow = _.template($('#selected-rows').html());

    $.get(dataUrl,
        {csrfmiddlewaretoken: csrftoken},
        function(res){
            _.each(res.data.tables,
                function(el){el['display'] = el['name'].substr(0, 23);});

            var rowsTemplate = _.template($('#database-rows').html());
                $('#databases').html(rowsTemplate({data: res.data})),
                dataWindow = $('#modal-data');

            chosenColsRows = $('#chosenColsRows');
            chosenTables = $('#chosenTables');
            chosenTables.html('');
            chosenColsRows.html('');
            dataWindow.modal('show');

            $('#tToR').addClass('disabled');
            $('#tsToR').addClass('disabled');
            $('#tToL').addClass('disabled');
            $('#tsToL').addClass('disabled');

            if(res.status == 'error'){
                confirmAlert(res.message);
            }
        }
    );
}

function checkChbs(div_id){
    var d = $('#'+div_id);
    if(d.attr('style')){
        d.removeAttr('style');
        $('#tToR').addClass('disabled');
    }

    var chbs = $('.mychb:checked');
    if(chbs.length){
        $('#tsToR').removeClass('disabled');
    }
    else{
        $('#tsToR').addClass('disabled');
    }
}

function setActive(div_id){
    var d = $('#'+div_id);
    if(d.attr('style')){
        d.removeAttr('style');
        $('.mychb').prop('checked', false);
        $('#tToR').addClass('disabled');

        var chbs = $('.mychb:checked');
        if(!chbs.length){
            $('#tsToR').addClass('disabled');
        }
    }
    else{
        $('.mychbdiv').removeAttr('style');
        $('.mychb').prop('checked', false);
        d.css('background-color', 'orange');
        d.find('input[type="checkbox"]').prop('checked', true);
        $('#tToR').removeClass('disabled');
    }
}

function getColumns(url, dict){
    $.get(url, dict,
        function(res){
          if (res.status == 'error') {
            confirmAlert(res.message);
          } else {
            chosenTables.append(colsTemplate({data: res.data}));
            chosenColsRows.append(colsNames({data: res.data}));
            $('#tToL').removeClass('disabled');
            $('#tsToL').removeClass('disabled');
          }
        }
    );
}

function tableToRight(url){
    var orange = $('div[style="background-color: orange;"]');

    if(orange.length && !$('#'+orange.attr('id')+'Cols').length){

        if(chosenColsRows.children().length>1){
            $('#sel-recs').remove();
        }

        getColumns(url, {
                    csrfmiddlewaretoken: csrftoken,
                    host: orange.attr('data-host'),
                    db : orange.attr('data-db'),
                    tables: JSON.stringify([orange.attr('data-table'), ])
                }
        );
    }
}

function tablesToRight(url){
    var divs = $('.mychb:checked').closest('div'),
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
        if(chosenColsRows.children().length>1){
            $('#sel-recs').remove();
        }

        dict['tables'] = JSON.stringify(tables);
        getColumns(url, dict);
    }
}

function addCol(tName, colName){
    $('#for-col-'+tName+'-'+colName).css('font-weight', 'bold');

    var col = $('#col-'+tName+'-'+colName);

    if(!col.length){
        chosenColsRows.append(
        colsNames({data: [{tname: tName, cols: [colName]}]}));
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
        $('#tToL').addClass('disabled');
        $('#tsToL').addClass('disabled');
    }
}

function tablesToLeft(){
    chosenTables.html('');
    chosenColsRows.html('');
    $('#tToL').addClass('disabled');
    $('#tsToL').addClass('disabled');
}

function refreshData(url){
    $('#sel-recs').remove();

    var source = $('#databases>div'),
        colsInfo = {
            "host": source.data("host"),
            "db": source.data("db"),
        },
        cols = $('#chosenColsRows>div>.select-col-div'),
        array = cols.map(function(){
            var el = $(this);
            return {
                "table": el.data("table"),
                "col": el.data("col")
            }
        }).get();

    if(array.length){
        colsInfo['cols'] = JSON.stringify(array);
        $.get(url, colsInfo, function(res){
            if (res.status == 'error') {
              confirmAlert(res.message)
            } else {
              $('#chosenColsRows').append(selectedRow({data: res.data}));
            }
        });
    }
}
