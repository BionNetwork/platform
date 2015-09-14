

function checkConnection(){
    var form = $('#conn_form'),
        formData = new FormData(form[0]),
        url = form.attr('data-url');
    console.log(formData);

    $.validator.messages.required = 'Обязательное поле!';

    form.valid();

    $.ajax({
        url: url,
        data: formData,
        processData: false,
        contentType: false,
        type: 'POST',
        success: function(data){
            console.log(data);
            if(data.result == 'error'){
                console.log('err');
                $.confirm({
                    text: "Подключение не удалось!",
                    title:"Внимание",
                    confirmButtonClass: "btn-danger",
                    cancelButtonClass: "hidden",
                    confirmButton: "Ок",
                });
            }
            else if(data.result == 'success'){
                $.confirm({
                    width: '100px',
                    text: "Подключение удалось!",
                    title:"Внимание",
                    cancelButtonClass: "hidden",
                    confirmButton: "Ок",
                });
            }
        }
    });
}

