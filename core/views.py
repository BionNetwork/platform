from django.shortcuts import render


# Create your views here.
def home(request):
    """App home view.
    """
    return render(request, "core/home.html")
