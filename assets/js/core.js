
var modalReg;

$(document).ready(function(){
        modalReg = $('#modal-register');
    }
);

function registration()
{
    modalReg.modal('show');
    $("#registration-message").attr("class", "alert").html("");
}

function registerUser(url){
    var f = $('#regi_form');
    if(!f.valid()){
        return;
    }
    var validator = f.data('validator');
    if($('[name="reg_password"]').val() != $('[name="reg_confirm"]').val()){
        validator.showErrors({'error': 'Не совпадение паролей'});
        return;
    }
    $.post(url, {
            csrfmiddlewaretoken: f.find('[name="csrfmiddlewaretoken"]').val(),
            email: $('[name="reg_email"]').val(),
            login: $('[name="reg_login"]').val(),
            password: $('[name="reg_password"]').val()
        }, function (response) {
            if (response['status'] == 'ok') {
                $("#modal-register .modal-body").html("<div class='alert alert-info'>" + response['message'] + "</div>");
            } else {
                $('#registration-message').addClass("alert-error").text(response['message'])
            }
        }
    );
}