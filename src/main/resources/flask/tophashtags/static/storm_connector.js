var source = new EventSource('/stream');
var list_div = document.getElementById("list");

source.onmessage = function (event) {

    var values = event.data.split("|");
    console.log(values);
    var html = "<ol>";
    for (j=0; j<values.length; j+=2) {
      html += "<li><span dir=\"auto\">" + values[j] + "</span> <span><b>[" +  values[j+1] + "]</b></span> </li>";
    }
    html += "</ol>"
    console.log(html);
    document.getElementById("list").innerHTML = html;
};
