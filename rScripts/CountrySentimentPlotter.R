library('ggplot2')
library('scales')
library('reshape2')
#library('forecast')
library('graphics')
#import data from delimite file
args<-commandArgs(TRUE)
data <- read.csv(file=args[1],sep='\t', header = FALSE)

# convert to date format
#data$V1 <- as.Date(data$V1)

#split string to extract year, month and day
data$x.year <- sapply(strsplit(as.character(data$V1),'-'), "[[", 1)
data$x.month <- sapply(strsplit(as.character(data$V1),'-'), "[[", 2)
data$x.sentiment <- sapply(strsplit(as.character(data$V1),'-'), "[[", 3)

# combine date and day in date format
data$x.axis <- as.Date(paste(data$x.month,'1', sep="-"), format="%m-%d")

g <- ggplot() + 
  # plot the first set of y values
  geom_line(data=data, aes(x=x.axis,y=V2, group=x.sentiment, color=x.sentiment)) + 
  #geom_line(data=data, aes(x=x.axis,y=V2)) + 
  # plot the second set of y values
  geom_point(data=data, aes(x=x.axis,y=V2,color=x.sentiment)) +
  # plot the second set of points
  facet_grid(x.year~.) +
  scale_color_manual(values=c("red", "blue", "black")) +
  # scale the x-axis as date and format label to show only month and day
  scale_x_date(labels = date_format("%b"),breaks = date_breaks("months"))

pdf(paste(args[1],".pdf",sep=""))
plot(g)
