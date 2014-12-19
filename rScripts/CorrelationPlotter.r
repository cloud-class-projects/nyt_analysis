library('ggplot2')
library('scales')
library('reshape2')
#library('forecast')
library('graphics')
#import data from delimite file
args<-commandArgs(TRUE)
data <- read.csv(file=args[1],sep='\t', header = FALSE)
#data2 <- read.csv(file='Microsoft_sentiment',sep='\t', header = FALSE)

# convert to date format
data$V1 <- as.Date(data$V1)
# data2$V1 <- as.Date(data2$V1)

#split string to extract year, month and day
data$x.year <- sapply(strsplit(as.character(data$V1),'-'), "[[", 1)
data$x.month <- sapply(strsplit(as.character(data$V1),'-'), "[[", 2)
data$x.day <- sapply(strsplit(as.character(data$V1),'-'), "[[", 3)

#data2$x.year <- sapply(strsplit(as.character(data2$V1),'-'), "[[", 1)
#data2$x.month <- sapply(strsplit(as.character(data2$V1),'-'), "[[", 2)
#data2$x.day <- sapply(strsplit(as.character(data2$V1),'-'), "[[", 3)

# combine date and day in date format
data$x.axis <- as.Date(paste(data$x.month,data$x.day,sep="-"), format= "%m-%d")
#data2$x.axis <- as.Date(paste(data2$x.month,data2$x.day,sep="-"), format= "%m-%d")


# plot the data
g <- ggplot() + 
  # plot the first set of y values
  # geom_line(aes(x=x.axis,y=V2, group=sentiment)) + 
  geom_line(data=data, aes(x=x.axis,y=V2)) + 
  # plot the second set of y values
  geom_line(data=data, aes(x=x.axis, y=V3),color="red") + 
  # plot the first set of points
  geom_point(data=data, aes(x=x.axis,y=V2,color=V2)) +
  # plot the second set of points
  geom_point(data=data, aes(x=x.axis,y=V3,color=V3)) +
  # facet grid for horizontal year (horizontal ~ vertical)
  facet_grid(x.year~.) + 
  # scale the x-axis as date and format label to show only month and day
  scale_x_date(labels = date_format("%b"),breaks = date_breaks("months"))


fit <- arima(x=data$V2,xreg=data$V3, order=c(1,0,0))
fit
pdf(paste(args[1],".pdf",sep=""))
tsdiag(fit)
plot(g)