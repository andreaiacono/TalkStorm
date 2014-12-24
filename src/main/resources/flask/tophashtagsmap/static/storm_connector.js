var source = new EventSource('/stream');
var top_hashtags = ['#hashtag1', '#hashtag2', '#hashtag3', '#hashtag4', '#hashtag5', '#hashtag6', '#hashtag7', '#hashtag8', '#hashtag9', '#hashtag10', '#hashtag11', '#hashtag12', '#hashtag13', '#hashtag14', '#hashtag15'];
var n = 20;

var colors= ['blue', 'brown', 'aqua', 'chocolate', 'fuchsia', 'gray', 'green', 'Indigo', 'lime', 'lightpink', 'maroon', 'navy', 'olive', 'orange', 'PaleGreen', 'purple', 'red', 'seagreen', 'silver', 'teal'];
var colors_number = colors.length;
var top_hashtags_div = document.getElementById("top_hashtags");
var tweet_div = document.getElementById("tweet");

source.onmessage = function (event) {

    var msg = event.data;
    var type = msg.split("|")[0];

    console.log("msg=" + msg);
    // data transmitted is the new top hashtags
    if (type == '0') {

        var values = msg.split("|");
        var html = "<b>Top N hashtags:</b><ol>";
        for (j=1; j<values.length; j+=2) {
            html += "<li><span dir=\"auto\" style=\"color:" + colors[top_hashtags.indexOf(values[j])] + "\">" + values[j] + "</span> <span><b>[" +  values[j+1] + "]</b></span> </li>";
            top_hashtags[Math.round(j/2)-1] = values[j];
        }
        html += "</ol>"

        top_hashtags_div.innerHTML = html;
    }
    // data transmitted is a tweet
    else if (type == '1') {

        var position = [msg.split("|")[2], msg.split("|")[1]];

        var lat = projection(position)[0];
        var lon = projection(position)[1];
        var hashtag = msg.split("|")[3];
        var tweet = msg.split("|")[4];

        console.log("hashtag=" + hashtag + "[" + top_hashtags.indexOf(hashtag) + "] msg=" + msg);
        var htmlTweet = "<b>Last tweet</b>";
        if (position[0] != '0' && position[1] != '0') {
            display_tweet(tweet, colors[top_hashtags.indexOf(hashtag)], [lat,lon]);
        }
        else {
            htmlTweet += " [No geo data]";
        }
        tweet_div.innerHTML = htmlTweet + ": <span style=\"color: " + colors[top_hashtags.indexOf(hashtag)] + "\">" + tweet + "</span>";
    }
};

function display_tweet(tweet, color, position) {
    svg.append("circle")
    .attr("r", 1)
    .attr("fill", color)
    .attr("stroke", "black")
    .attr("cx",position[0])
    .attr("cy",position[1])
    .transition()
    .attr("r", 20)
    .transition()
    .attr("r", 0)
    .remove();

    /*
    svg.append("text")
    .attr("font-family", "sans-serif")
    .attr("font-size", 0)
    .attr("x", 100)
    .attr("y", 100)
    .transition()
    .attr("font-size", 30)
    .text("Provola #ciao faccimo la pov ai di n teweet olto lungoe vediamo cosa succede anche e mi sono dimeticat degli spazi qua e lò,")
    .delay(2000)
    .transition()
    .attr("font-size", 0)
    .remove();
    */
}