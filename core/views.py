# coding: utf-8
from __future__ import unicode_literals

import uuid
import json
import logging
from PIL import Image
import StringIO
from datetime import datetime

from django.core.files.uploadedfile import InMemoryUploadedFile
from django.contrib.auth import authenticate, login, logout
from django.utils.decorators import method_decorator
from django.contrib.auth.decorators import login_required
from django.shortcuts import render, get_object_or_404
from django.views.generic import TemplateView, View
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponseRedirect
from django.core.urlresolvers import reverse
# from django.contrib.auth.models import check_password
from django.conf import settings
from django.core.mail import send_mail
from django.http import HttpResponse
from django.db.models import Q, DateField
from django.core.paginator import Paginator

from django.core.management.commands import loaddata

from smtplib import SMTPServerDisconnected
from .models import User
from .helpers import Settings, CustomJsonEncoder
from . import forms as core_forms

logger = logging.getLogger(__name__)


class BaseView(View):
    """
    Базовый класс для View
    """
    @method_decorator(login_required)
    def dispatch(self, *args, **kwargs):
        return super(BaseView, self).dispatch(*args, **kwargs)

    def redirect(self, reverse_name, args=None, **kwargs):
        return HttpResponseRedirect(reverse(reverse_name, args=args), **kwargs)

    def redirect_to_url(self, url, **kwargs):
        return HttpResponseRedirect(url, **kwargs)

    def json_response(self, context, **response_kwargs):
        response_kwargs['content_type'] = 'application/json'
        return HttpResponse(
            json.dumps(context, cls=CustomJsonEncoder), **response_kwargs)


class JSONMixin(object):
    def render_to_response(self, context, **httpresponse_kwargs):
        return self.get_json_response(
            self.convert_context_to_json(context),
            **httpresponse_kwargs
        )

    def get_json_response(self, content, **httpresponse_kwargs):
        return HttpResponse(
            content,
            content_type='application/json',
            **httpresponse_kwargs
        )

    def convert_context_to_json(self, context):
        u""" This method serialises a Django form and
        returns JSON object with its fields and errors
        """
        forms = {}
        for form_name, form in context.iteritems():
            form = context.get('form')
            to_json = {}
            options = context.get('options', {})
            to_json.update(options=options)
            to_json.update(success=context.get('success', False))
            fields = {}
            for field_name, field in form.fields.items():
                if isinstance(field, DateField) \
                        and isinstance(form[field_name].value(), datetime.date):
                    fields[field_name] = \
                        unicode(form[field_name].value().strftime('%d.%m.%Y'))
                else:
                    fields[field_name] = \
                        form[field_name].value() \
                        and unicode(form[field_name].value()) \
                        or form[field_name].value()
            to_json.update(fields=fields)
            if form.errors:
                errors = {
                    'non_field_errors': form.non_field_errors(),
                }
                fields = {}
                for field_name, text in form.errors.items():
                    fields[field_name] = text
                errors.update(fields=fields)
                to_json.update(errors=errors)
            forms.update({form_name: to_json})
        return json.dumps(forms)


class BaseViewNoLogin(BaseView):
    """
    Базовый класс для View без аутентификации
    """
    @csrf_exempt
    def dispatch(self, *args, **kwargs):
        return super(BaseView, self).dispatch(*args, **kwargs)


class BaseTemplateView(TemplateView, BaseView):
    """
    Базовый класс для View для работ с template
    """
    pass


class HomeView(BaseTemplateView):
    """Главная страница dashboard"""
    @method_decorator(login_required)
    def dispatch(self, *args, **kwargs):
        return super(HomeView, self).dispatch(*args, **kwargs)

    def get(self, request, *args, **kwargs):
        return render(request, "index.html")


class AngularView(BaseTemplateView):
    """Главная страница dashboard"""
    def get(self, request, *args, **kwargs):
        return render(request, "index.html")


class LoginView(BaseTemplateView):
    """
    Вход пользователя в систему
    """
    template_name = 'core/login.html'

    def dispatch(self, *args, **kwargs):
        return super(BaseView, self).dispatch(*args, **kwargs)

    def get(self, request, *args, **kwargs):
        if request.user.is_authenticated():
            return self.redirect_to_url("/")
        if "next" in request.GET:
            next_url = request.GET['next']
        else:
            next_url = None
        return self.render_to_response({'regUrl': '/registration', 'next': next_url})

    def post(self, request, *args, **kwargs):
        post = request.POST

        username = post['username']
        password = post['password']
        user = None

        if "next" in request.POST:
            next_url = request.POST['next']
        else:
            next_url = None

        # if email (костылим):
        if '@' in username:
            users = User.objects.filter(email=username)
            for us in users:
                if check_password(password, us.password):
                    user = authenticate(
                        username=us.username,
                        password=password
                    )
                    break
        else:
            user = authenticate(
                username=username,
                password=password
            )

        if user is not None:
            if user.is_active:
                login(request, user)
                if next_url:
                    return self.redirect_to_url(next_url)
                else:
                    return self.redirect("core:home")
            else:
                if not user.email:
                    return self.render_to_response({
                        'error': 'Пользователь не имеет почты!'
                    })

                host = Settings.get_host(request)
                code = uuid.uuid4().hex
                message = '{0}{2}?uuid={1}'.format(host, code, reverse('core:activate_user'))

                user.verify_email_uuid = code
                user.save()

                send_mail('Подтверждение!', message, settings.EMAIL_HOST_USER,
                          [user.email], fail_silently=False)

                return self.render_to_response({
                    'error': 'Пользователь заблокирован! Проверьте свою почту!'
                })

        return self.render_to_response({
            'error': 'Неправильный логин или пароль!',
            'next': next_url
        })


class LogoutView(BaseView):
    """
    Выход пользователя
    """

    def dispatch(self, *args, **kwargs):
        return super(BaseView, self).dispatch(*args, **kwargs)

    def get(self, request, *args, **kwargs):
        logout(request)
        return self.redirect('login')


class RegistrationView(BaseView):
    """
    Регистрация нового пользователя в системе
    """
    # у предка dispatch перекрыт login_required
    def dispatch(self, *args, **kwargs):
        return super(BaseView, self).dispatch(*args, **kwargs)

    def post(self, request, *args, **kwargs):

        post = request.POST

        host = Settings.get_host(request)
        code = uuid.uuid4().hex
        message = '{0}{2}?uuid={1}'.format(host, code, reverse('core:activate_user'))

        user_login = post.get('login', '')
        user_email = post.get('email', '')

        try:
            if len(user_login) == 0 or len(user_email) == 0:
                raise ValueError("Логин или пароль не могут быть пустыми")

            current_user = User.objects.filter(Q(username=user_login) | Q(email=user_email))

            if len(current_user) > 0:
                raise ValueError("Такой пользователь уже существует")
            user = User(
                username=user_login,
                email=user_email,
                is_active=False,
                is_staff=True,
                verify_email_uuid=code
            )
            user.set_password(post.get('password'))
            user.save()

            # тут сделать очередь на письма
            send_mail('Подтверждение регистрации!', message, settings.EMAIL_HOST_USER,
                      [user.email], fail_silently=False)

            return self.json_response(
                {'status': 'ok',
                 'message': 'Регистрация прошла успешно! На почту была отправлена инструкция по активации аккаунта.'})
        except ValueError as e:
            return self.json_response(
                {'status': 'error', 'message': e.message}
            )
        except SMTPServerDisconnected as e:
            logger.exception(e.message)
            return self.json_response(
                {'status': 'error', 'message': "Ошибка при отправке почты %s" % e.message}
            )
        except:
            logger.exception("Произошла системная ошибка")
            return self.json_response(
                {'status': 'error', 'message': 'Произошла системная ошибка. Мы уже работаем над ней'}
            )


class SetUserActive(BaseView):
    """
    Активация пользователей
    """
    # тут насувать мессаджей на логин page

    def get(self, request, *args, **kwargs):
        uuid4 = request.GET.get('uuid', None)
        if not uuid4:
            return self.redirect('login')

        try:
            user = User.objects.get(verify_email_uuid=uuid4)
        except User.DoesNotExist:
            return self.redirect('login')
        else:
            user.is_active = True
            user.verify_email_uuid = None
            user.save()

        user.backend = 'django.contrib.auth.backends.ModelBackend'

        login(request, user)

        return self.redirect('home')


class UserListView(BaseTemplateView):
    """
    Список пользователей в системе
    """
    template_name = 'core/users/index.html'

    def get(self, request, *args, **kwargs):

        get = request.GET
        or_cond = Q()

        search = get.get('search', None)
        if search:
            for field in ('username', 'email', 'id'):
                or_cond |= Q(
                    **{"%s__icontains" % field: search}
                )
        users = User.objects.filter(or_cond)
        count = 20

        paginator = Paginator(users, count)
        page_count = paginator.num_pages

        page = int(get.get('page', 0))
        if page not in xrange(page_count):
            page = 0

        users = paginator.page(page + 1)

        return self.render_to_response(
            {
                'users': users,
                'range': range(page_count),
                'page': page,
                'max': page_count-1,
                'search': search or ''
            }
        )


class UserListViewRest(BaseTemplateView):

    def get(self, request, *args, **kwargs):
        get = request.GET
        or_cond = Q()

        search = get.get('search', None)
        if search:
            for field in ('username', 'email', 'id'):
                or_cond |= Q(
                    **{"%s__icontains" % field: search}
                )
        users = User.objects.filter(or_cond)
        count = 20

        paginator = Paginator(users, count)
        page_count = paginator.num_pages

        page = int(get.get('page', 0))
        if page not in xrange(page_count):
            page = 0

        users = paginator.page(page + 1)

        return self.json_response(
            {
                'users': users,
                'range': range(page_count),
                'page': page,
                'max': page_count-1,
                'search': search or ''
            }
        )


class RemoveUserView(BaseView):
    """
    Удаление пользователя
    """
    def post(self, request, *args, **kwargs):
        user = get_object_or_404(User, pk=kwargs.get('id'))
        user.delete()

        return self.json_response({'redirect_url': reverse('core:users')})


class NewUserView(BaseTemplateView):
    """
    Создание нового пользователя
    """
    template_name = 'core/users/add.html'

    def get(self, request, *args, **kwargs):
        form = core_forms.NewUserForm()
        return render(request, self.template_name, {'form': form})

    def post(self, request, *args, **kwargs):
        post = request.POST
        form = core_forms.NewUserForm(post)

        if not form.is_valid():
            return self.render_to_response({'form': form})

        user = form.save(commit=False)
        user.is_active = False
        user.is_staff = True
        user.set_password(post.get('password'))
        user.save()

        return self.redirect('core:users')


class EditUserView(BaseTemplateView):
    """
    Редактирование пользователя
    """
    template_name = 'core/users/edit.html'

    def get(self, request, *args, **kwargs):

        user = get_object_or_404(User, pk=kwargs.get('id'))

        form = core_forms.UserForm(instance=user)
        return self.render_to_response({
            'form': form,
            'first_tab': ['avatar', 'username', 'email', ],
            'full_path': request.get_full_path(),
        })

    def post(self, request, *args, **kwargs):

        post = request.POST
        user = get_object_or_404(User, pk=kwargs.get('id'))
        form = core_forms.UserForm(post, instance=user)

        if not form.is_valid():
            return self.render_to_response({
                'form': form,
                'first_tab': ['avatar', 'username', 'email', ],
                'full_path': request.get_full_path(),
            })

        profile = form.save(commit=False)

        image = request.FILES.get('file', None)

        # добавили аву
        if image:
            filename = image.name
            sm_filename = 'small_'+filename

            avatar_img = Image.open(image)
            avatar_img.thumbnail(settings.AVATAR_SIZES, Image.ANTIALIAS)
            big_img_io = StringIO.StringIO()
            avatar_img.save(big_img_io, format='JPEG')
            avatar = InMemoryUploadedFile(big_img_io, None, filename,
                                          'image/jpeg', big_img_io.len, None)
            user.avatar.delete()
            user.avatar.save(filename, avatar)

            small_avatar_img = Image.open(image)
            small_avatar_img.thumbnail(settings.SMALL_AVATAR_SIZES, Image.ANTIALIAS)
            small_img_io = StringIO.StringIO()
            small_avatar_img.save(small_img_io, format='JPEG')
            small_avatar = InMemoryUploadedFile(
                small_img_io, None, sm_filename,
                'image/jpeg', small_img_io.len, None)
            user.avatar_small.delete()
            user.avatar_small.save(sm_filename, small_avatar)

        # ничего не меняли
        elif not post.get('fileupload_avatar'):
            user.avatar.delete()
            user.avatar_small.delete()

        profile.save()

        next_url = post.get('full_path', None)
        if next_url and '?next=' in next_url:
            return HttpResponseRedirect(next_url.split('?next=')[1])
        else:
            return self.redirect('core:users')


class RedirectToProfileView(EditUserView):

    def post(self, request, *args, **kwargs):
        super(RedirectToProfileView, self).post(request, *args, **kwargs)

        return self.redirect('users.profile', (request.user.id, ))


class UserProfileView(BaseTemplateView):
    """
    Страница профиля пользователя
    """
    template_name = 'core/users/profile.html'

    def get(self, request, *args, **kwargs):
        user = get_object_or_404(User, pk=kwargs.get('id'))

        return self.render_to_response({
            'user': user
        })

    def post(self, request, *args, **kwargs):
        pass
