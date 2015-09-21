
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

var chosenTables, colsTemplate;

function getConnectionData(dataUrl){

    colsTemplate = _.template($('#table-cols').html())

    $.get(dataUrl,
        {csrfmiddlewaretoken: csrftoken},
        function(res){
            var rowsTemplate = _.template($('#database-rows').html());
                $('#databases').html(rowsTemplate({data: res.data})),
                dataWindow = $('#modal-data');

            chosenTables = $('#chosenTables');
            chosenTables.html('');
            dataWindow.modal('show');

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
            if(res.message){
               confirmAlert(res.message);
            }
            else{
               chosenTables.append(colsTemplate({data: res.data}));
            }
        }
    );
}

function tableToRight(url){
    var orange = $('div[style="background-color: orange;"]');

    if(orange.length && !$('#'+orange.attr('id')+'Cols').length){
        getColumns(url, {
                    csrfmiddlewaretoken: csrftoken,
                    host: orange.attr('data-host'),
                    db : orange.attr('data-db'),
                    tables: JSON.stringify([orange.attr('data-table'), ])});
                   }
}

function tablesToRight(url){
    var divs = $('.mychb:checked').closest('div'),
        dict = {
                csrfmiddlewaretoken: csrftoken,
                host: divs.attr('data-host'),
                db : divs.attr('data-db'),
            }

    var tables = divs.map(function(i, el) {
        var id = $(this).attr('id');
        if(!$('#'+id+'Cols').length){
            return $(this).attr('data-table');
        }
    }).get();

    dict['tables'] = JSON.stringify(tables);

    getColumns(url, dict);
}