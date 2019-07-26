**Scenario**:

Time series does not have a trend line and does not have seasonality component. We would use a Simple Exponential Smoothing model.

**Simple Exponential Smoothing Explained**

For simple exponential smoothing methods, the forecast is calculated by multiplying past values by relative weights, which are calculated based upon what is termed a smoothing parameter. You’ll also hear this called the **alpha** or **α**. This is the magnitude of the weight applied to the previous values, with the weights decreasing exponentially as the observations get older. The formula looks like this:

**Forecast = Weightt Yt + Weightt-1 Yt-1 + Weightt-2 Yt-2 + ... + (1-α)n Yn**

where

**t** is the number of time periods before the most recent period (e.g. t = 0 for the most recent time period, t = 1 for the time period before that).

**Yt** = actual value of the time series in period t

**Weightt** = α(1-α)t

**n** = the total number of time periods

This model basically gives us a smooth line or LEVEL in our forecast that we can use to forecast the next period.

Here are a few key points to help understand the smoothing parameter:

The smoothing parameter can be set for any value between 0 and 1.
If the smoothing parameter is close to one, more recent observations carry more weight or influence over the forecast (if α = 0.8, weights are 0.8, 0.16, 0.03, 0.01, etc.).
If the smoothing parameter is close to zero, the influence or weight of recent and older observations is more balanced (if α = 0.2, weights are 0.2, 0.16, 0.13, 0.10, etc.).
Choosing the Smoothing Parameter α
Choosing the correct smoothing parameter is often an iterative process. Luckily, advanced statistical tools, like Alteryx, will select the best smoothing parameter based upon minimizing forecasting error. Otherwise, you will need to test many smoothing parameters against each other to see which model best fits the data.

The advantage of exponential smoothing methods over simple moving averages is that new data is depreciated at a constant rate, gradually declining in its impact, whereas the impact of a large or small value in a moving average, will have a constant impact. However, this also means that exponential smoothing methods are more sensitive to sudden large or small values.

The simple exponential smoothing method does not account for any trend or seasonal components, rather, it only uses the decreasing weights to forecast future results. This makes the method suitable only for time series without trend and seasonality.

Want to learn more?
To ready more about the math behind simple exponential smoothing, see here. 
To learn how to do simple exponential smoothing in Microsoft Excel, see here
