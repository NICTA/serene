#compute AUC
#need to provide parameters: labels, predictions

library("pROC")

folder      <- "%s"
filename    <- "%s"
labels      <- c(%s)
predictions <- c(%s)

pdf(sprintf("%%s/%%s.pdf", folder,filename))
plot.roc(labels,predictions)
dev.off()

auc(labels, predictions)