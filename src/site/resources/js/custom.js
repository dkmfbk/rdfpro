$(window).load(function(){

    $('a[href^="#"]:not([href^="#carousel"]):not([data-toggle="dropdown"])').on('click', function(e) {
        $('html, body').animate({
            scrollTop: $(this.hash).offset().top - 80
        }, 300);
    });

    $(document).ready(function () {
        var url = window.location.href.split('#')[0];
        var path = url.split('/');
        var file = path[path.length - 1];
        $('.nav').each(function(e) {
            $(this).find('li').each(function(li) {
                if (file != "index.html" && file != "team.html" || !$(this).text().includes("Maven Reports")) {
                    if ($(this).find('a[href="' + url + '"], a[href="' + file + '"]').size() > 0) {
                        $(this).addClass("active");
                    }
                }
            });
        });
        // $("pre.source").addClass("prettyprint");
        prettyPrint();
    });

});
