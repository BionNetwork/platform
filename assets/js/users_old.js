

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


var activePage, usersUrl;

$(document).ready(function(){
    activePage = $('#active_num').val();
    usersUrl = $('#users_table').attr('data-url');

    $('[name="birth_date"]').datepicker({
        format: 'dd.mm.yyyy',
        autoclose: true,
        forseParse: false,
        keyboardNavigation: false
    });

   $('[name="phone"]').inputmask("99999999999");

});

function prev(){
    var search = $('#search').val();
    activePage = activePage==0 ? activePage : activePage - 1
    document.location = usersUrl+'?page='+activePage+';search='+search;
}

function next(){
    var search = $('#search').val(),
        current = $("li.active[data-pagi='true']");
    if(!current.attr('data-max')){
        activePage = activePage + 1
    }
    document.location = usersUrl+'?page='+activePage+';search='+search;
}

function pagi(ind){
    var search = $('#search').val();
    document.location = usersUrl+'?page='+ind+';search='+search;
}

function search(){
    var search = $('#search').val();
    document.location = usersUrl+'?search='+search;
}