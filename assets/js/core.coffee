modalReg = null

$(document).ready( ->
  modalReg = $('#modal-register');
);

class UserService
  @showRegistration: ->
    modalReg.modal('show');
    $("#registration-message").attr("class", "alert").html("");
  @register: (url) ->
    regForm = $('#regi_form');
    if(!regForm.valid())
        return false
    validator = regForm.data('validator')
    if($('[name="reg_password"]').val() != $('[name="reg_confirm"]').val())
        validator.showErrors({'error': 'Не совпадение паролей'})
        return false

    $.post(url, {
            csrfmiddlewaretoken: regForm.find('[name="csrfmiddlewaretoken"]').val(),
            email: $('[name="reg_email"]').val(),
            login: $('[name="reg_login"]').val(),
            password: $('[name="reg_password"]').val()
        }, (response) ->
            if (response['status'] == 'ok')
                $("#modal-register .modal-body").html("<div class='alert alert-info'>" + response['message'] + "</div>");
            else
                $('#registration-message').addClass("alert-error").text(response['message'])
    );