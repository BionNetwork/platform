

function removeUser(url){
    $.confirm({
        text: "Вы действительно хотите удалить пользователя?",
        confirm: function(button) {
            $.post(url,
                {csrfmiddlewaretoken: csrftoken},
                function(data) {
                    window.location = data.redirect_url;
                }
            );
        },
        title:"Удаление пользователя",
        confirmButton: "Удалить",
        cancelButton: "Отмена"
    });
}