var source = new EventSource('/stream');
var n = 20;

var tweet_div = document.getElementById("tweet");

source.onmessage = function (event) {

    var msg = event.data;

    console.log("msg=" + msg);
    var position = [msg.split("|")[1], msg.split("|")[0]];

    var lat = projection(position)[0];
    var lon = projection(position)[1];
    var tweet = msg.split("|")[2];

    var htmlTweet = "<b>Last tweet</b>";
    if (position[0] != '0' && position[1] != '0') {
        display_tweet(tweet, [lat,lon]);
    }
    else {
        htmlTweet += " [No geo data]";
    }
    tweet_div.innerHTML = htmlTweet + ": <span style=\"color: green\">" + tweet + "</span>";
};

function display_tweet(tweet, position) {
    svg.append("circle")
    .attr("r", 1)
    .attr("fill", "green")
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
