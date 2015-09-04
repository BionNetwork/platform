
var modalReg;

$(document).ready(function(){
        modalReg = $('#modal-register');
    }
);

function registration(){
    modalReg.modal('show');
}

function registerUser(regUrl){
    var f = $('#regi_form');
    if(!f.valid()){
        return;
    }
    var validator = f.data('validator');
    if($('[name="reg_password"]').val() != $('[name="reg_confirm"]').val()){
        validator.showErrors({'error': 'Не совпадение паролей'});
        return;
    }
    $.post(regUrl, {
        csrfmiddlewaretoken: f.find('[name="csrfmiddlewaretoken"]').val(),
        email: $('[name="reg_email"]').val(),
        login: $('[name="reg_login"]').val(),
        password: $('[name="reg_password"]').val(),
        }, function(res){
            modalReg.modal('hide');
            $('[name="error"]').text(res['error'])
        }
    );
}