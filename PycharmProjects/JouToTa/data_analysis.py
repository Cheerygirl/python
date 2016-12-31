#!/usr/bin/python
# -*- coding: utf-8 -*
__author__ = 'Cheery'

#Imports
#dataprocessing & viturlization
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style('whitegrid')

#machine learning
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC, LinearSVC
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB

#data extracting
titanic_df = pd.read_csv('F:/kaggle/titanic/train.csv',dtype={'Age':np.float64},)
test_df = pd.read_csv('F:/kaggle/titanic/test.csv',dtype={'Age':np.float64},)

#data_check
#titanic_df.head()
#titanic_df.info()
#print('---------------')
#test_df.info()

#drop unnecessary columns
#these columns won't be useful in analysis and prediction
titanic_df = titanic_df.drop(['PassengerId','Name','Ticket'],axis=1)
test_df = test_df.drop(['PassengerId','Name','Ticket'],axis=1)

###########Embarked###################
#only in titanic_df, fill the 2 missiong values with the most occurred value, which is "S"
titanic_df['Embarked'] = titanic_df['Embarked'].fillna('S')

embark_cnt = titanic_df[["Embarked", "Survived"]].groupby(['Embarked'],as_index=False).count()
embark_suvcnt = titanic_df[["Embarked", "Survived"]].groupby(['Embarked'],as_index=False).sum()
embark_perc = titanic_df[["Embarked", "Survived"]].groupby(['Embarked'],as_index=False).mean()

#Either to consider Embarked column in predictions,
#and remove "S" dummy variable,
#and leave "C" & "Q", since they seem to have a good rate for Survival.
###OR, don't create dummy variables for Embarked column, just drop it,
###because logically, Embarked doesn't seem to be useful in prediction.

embark_dummies_titanic  = pd.get_dummies(titanic_df['Embarked'])
embark_dummies_titanic.drop(['S'], axis=1, inplace=True)

embark_dummies_test  = pd.get_dummies(test_df['Embarked'])
embark_dummies_test.drop(['S'], axis=1, inplace=True)

titanic_df = titanic_df.join(embark_dummies_titanic)
test_df = test_df.join(embark_dummies_test)

titanic_df.drop(['Embarked'], axis=1, inplace=True)
test_df.drop(['Embarked'], axis=1, inplace=True)

###########Fare##################
#only for test_df, since there is a missiong "Fare" values
test_df['Fare'].fillna(test_df['Fare'].median(), inplace=True)

#type
titanic_df['Fare'] = titanic_df['Fare'].astype(int)
test_df['Fare'] = test_df['Fare'].astype(int)

#fare analysis
fare_not_survived = titanic_df['Fare'][titanic_df['Survived']==0]
fare_survived = titanic_df['Fare'][titanic_df['Survived']==1]

avgerage_fare = pd.DataFrame([fare_not_survived.mean(),fare_survived.mean()])
std_fare = pd.DataFrame([fare_not_survived.std(),fare_survived.std()])

titanic_df['Fare'].plot(kind='hist', figsize=(15,3),bins=100, xlim=(0,50))

avgerage_fare.index.names = std_fare.index.names = ['Survived']
avgerage_fare.plot(yerr=std_fare,kind='bar',legend=False)

########Age############
#plot setting
fig, (axis1,axis2) = plt.subplots(1,2,figsize=(15,4))
axis1.set_title('Original Age values - Titanic')
axis2.set_title('New Age values - Titanic')

#data processing
average_age_titanic = titanic_df['Age'].mean()
std_age_titanic = titanic_df['Age'].std()
count_nan_age_titanic = titanic_df['Age'].isnull().sum()

average_age_test = test_df['Age'].mean()
std_age_test = test_df['Age'].std()
count_age_test = test_df['Age'].isnull().sum()

rand_1 = np.random.randint(average_age_titanic-std_age_titanic, average_age_titanic+std_age_titanic, size=count_nan_age_titanic)
rand_2 = np.random.randint(average_age_test-std_age_test, average_age_test+std_age_test, size=count_age_test)

#plot  real_age
titanic_df['Age'].dropna().astype(int).hist(bins=70,ax=axis1)
#test_df['Age'].dropna().astype(int).hist(bins=70,ax=axis1)

#fill NaN values in Age column with random values generated
titanic_df['Age'][np.isnan(titanic_df['Age'])] = rand_1
test_df['Age'][np.isnan(titanic_df['Age'])] = rand_2

titanic_df['Age'] = titanic_df['Age'].astype(int)
test_df['Age'] = test_df['Age'].astype(int)

#plot new_age with nan deals
titanic_df['Age'].hist(bins=70,ax=axis2)
#test_df['Age'].hist(bins=70,ax=axis2)

#data analysis
#peaks for survived/not survived passengers by their age
facet = sns.FacetGrid(titanic_df, hue="Survived",aspect=4)
facet.map(sns.kdeplot,'Age',shade= True)
facet.set(xlim=(0, titanic_df['Age'].max()))
facet.add_legend()

# average survived passengers by age
fig, axis1 = plt.subplots(1,1,figsize=(18,4))
average_age = titanic_df[['Age','Survived']].groupby(['Age'],as_index=False).mean()
sns.barplot(x='Age', y='Survived', data=average_age)

##########Cabin##########
#It has lots of NaN values, so it won't cause a remarkable impact on prediction
titanic_df.drop('Cabin',axis=1,inplace=True)
test_df.drop('Cabin',axis=1,inplace=True)

######Family#########
#Instead of having two columns Parch&Sibsp,
#we can have only one columns represent if the passenger had any family member aboard or not,
#Meaning, if having any family member will increase chances of Survival or not
titanic_df['Family'] = titanic_df['Parch'] + titanic_df['SibSp']
titanic_df['Family'].loc[titanic_df['Family']>0] = 1
titanic_df['Family'].loc[titanic_df['Family']==0] = 0

test_df['Family'] = test_df['Parch']+test_df['SibSp']
test_df['Family'].loc[test_df['Family']>0] = 1
test_df['Family'].loc[test_df['Family']==0] = 0

#drop SibSp & Parch
titanic_df = titanic_df.drop(['SibSp','Parch'], axis=1)
test_df = test_df.drop(['SibSp','Parch'],axis=1)

# plot
fig, (axis1,axis2) = plt.subplots(1,2,sharex=True,figsize=(10,5))
sns.countplot(x='Family', data=titanic_df, order=[1,0], ax=axis1)

family_perc = titanic_df[["Family", "Survived"]].groupby(['Family'],as_index=False).mean()
sns.barplot(x='Family', y='Survived', data=family_perc, order=[1,0], ax=axis2)

axis1.set_xticklabels(["With Family","Alone"], rotation=0)

#########Sex###########
#As we see, Children(age<16)on aboard seem to have a high chances for Survival
#So, we can classify passengers as males, females and child
def get_person(passenger):
    age,sex = passenger
    return 'child' if age < 16 else sex

titanic_df['Person'] = titanic_df[['Age','Sex']].apply(get_person,axis=1)
test_df['Person'] = test_df[['Age','Sex']].apply(get_person,axis=1)

#drop Sex
titanic_df.drop(['Sex'],axis=1,inplace=True)
test_df.drop(['Sex'],axis=1,inplace=True)

#create dummy variables for Person column
#drop Male as it has the lowest average of survived passengers
person_dummies_titanic = pd.get_dummies(titanic_df['Person'])
person_dummies_titanic.columns = ['Child','Female','Male']
person_dummies_titanic.drop(['Male'],axis=1,inplace=True)

person_dummies_test = pd.get_dummies(test_df['Person'])
person_dummies_test.columns = ['Child','Female','Male']
person_dummies_test.drop(['Male'],axis=1,inplace=True)

titanic_df = titanic_df.join(person_dummies_titanic)
test_df = test_df.join(person_dummies_test)

person_perc = titanic_df[['Person','Survived']].groupby(['Person'],index=False).mean()
#plot
fig, (axis1,axis2) = plt.subplots(1,2,figsize=(10,5))
# sns.factorplot('Person',data=titanic_df,kind='count',ax=axis1)
sns.countplot(x='Person', data=titanic_df, ax=axis1)
sns.barplot(x='Person',y='Survived',data=embark_perc,ax=axis2,order=['male','female','child'])

titanic_df.drop(['Person'],axis=1,inplace=True)
test_df.drop(['Person'],axis=1,inplace=True)

#########Pclass##############
sns.factorplot('Pclass','Survived',order=[1,2,3],data=titanic_df,size=5)

#Create dummy variables for Pclass column
#drop 3rd class as it has the lowest averge of survived passenges
pclass_dummies_titanic = pd.get_dummies(titanic_df['Pclass'])
pclass_dummies_titanic.columns = ['Class_1','Class_2','Class_3']
pclass_dummies_titanic.drop(['Class_3'],axis=1,inplace=True)

pclass_dummies_test = pd.get_dummies(test_df['Pclass'])
pclass_dummies_test.columns = ['Class_1','Class_2','Class_3']
pclass_dummies_test.drop(['Class_3'],axis=1,inplace=True)

titanic_df.drop(['Pclass'],axis=1,inplace=True)
test_df.drop(['Pclass'],axis=1,inplace=True)

titanic_df = titanic_df.join(pclass_dummies_titanic)
test_df = test_df.join(pclass_dummies_test)

###################################################
#define training and testing sets
##################################################
X_train = titanic_df.drop('Survived',axis=1)
Y_train = titanic_df['Survived']
X_test = test_df.drop('PassengerId',axis=1).copy()

#####Logistic Regression
logreg = LogisticRegression()
logreg.fit(X_train,Y_train)
Y_pred = logreg.predict(X_test)
logreg.score(X_train,Y_train)

#####Support Vector Machines
svc = SVC()
svc.fit(X_train,Y_train)
Y_pred = svc.predict(X_test)
svc.score(X_train,Y_train)

######Random Forests
random_forest = RandomForestClassifier(n_estimators=100)
random_forest.fit(X_train,Y_train)
Y_pred = random_forest.predict(X_test)
random_forest.score(X_train,Y_train)

######KNeighborsClassifier
knn = KNeighborsClassifier(n_neighbors=3)
knn.fit(X_train,Y_train)
Y_pred = knn.predict(X_test)
knn.score(X_train,Y_train)

#########Gaussian Naive Bayes
gaussian = GaussianNB()
gaussian.fit(X_train,Y_train)
Y_pred = gaussian.predict(X_test)
gaussian.score(X_train,Y_train)

# get Correlation Coefficient for each feature using Logistic Regression
coeff_df = pd.DataFrame(titanic_df.columns.delete(0))
coeff_df.columns = ['Features']
coeff_df["Coefficient Estimate"] = pd.Series(logreg.coef_[0])

# preview
coeff_df

submission = pd.DataFrame({
        "PassengerId": test_df["PassengerId"],
        "Survived": Y_pred
    })
submission.to_csv('titanic.csv', index=False)