#! /bin/bash

# A POSIX variable
# Reset in case getopts has been used previously in the shell.
OPTIND=1

# Initialize variables:
while getopts i:o:p: option
do
    case "${option}" in
        i) INPUT=${OPTARG};;
        o) OUTPUT=${OPTARG};;
        p) PROCESS=${OPTARG};;
    esac
done

shift $((OPTIND-1))
[ "${1:-}" = "--" ] && shift


echo 'InputFile='$INPUT
echo 'OutputDir='$OUTPUT 
echo 'Process='$PROCESS
if [ $PROCESS == 'infer' ]
then
    ALL='./model_all'
    INFER=$ALL'/inferencer.model'
    echo 'AllDir='$ALL
    echo 'Inferencer'=$INFER
fi

# set path
#INPUT='../data/train/dataset.csv'
#OUTPUT='./model_all'

# set process
#PROCESS='train'
#PROCESS='infer'

# set arg
SEED1=1
SEED2=1

# configure mallet train-topics parameters:

#--num-top-docs INTEGER
#  When writing topic documents with --output-topic-docs, report this number of top documents.
#  Default is 100
TOPICS=500

#--num-iterations INTEGER
#  The number of iterations of Gibbs sampling.
#  Default is 1000
#ITERATION=500
ITERATION=2000
#ITERATION=200

#--optimize-interval INTEGER
#  The number of iterations between reestimating dirichlet hyperparameters.
#  Default is 0
INTERVAL=40
#INTERVAL=10

#--optimize-burn-in INTEGER
#  The number of iterations to run before first estimating dirichlet hyperparameters.
#  Default is 200
BURNIN=300
#BURNIN=20

echo 'SEED1='$SEED1
echo 'SEED2='$SEED2
echo 'TOPICS='$TOPICS
echo 'ITERATION='$ITERATION
echo 'INTERVAL='$INTERVAL
echo 'BURNIN='$BURNIN


# Import corpus
echo $( date +%T)' :: Start import dataset...'
if [ ! -f $OUTPUT/import.model ]
then
    if [ $PROCESS == 'train' ]
    then
    #    mallet import-file --input $INPUT \
    #                       --output $OUTPUT/import.model \
    #                       --label 0 \
    #                       --remove-stopwords \
    #                       --keep-sequence
    #                       #--keep-sequence-bigrams
    #                       #--token-regex '\p{L}[\p{L}\p{P}]+\p{L}'
    #                       #--token-regex '\p{L}[\p{L}\p{P}]\p{L}+' \
    #                       #--token-regex '[a-zA-Z]{4,15}'
        echo 'Import new data for training.'
    elif [ $PROCESS == 'infer' ]
    then
    #    mallet import-file --input $INPUT \
    #                       --output $OUTPUT/import.model \
    #                       --use-pipe-from $ALL/import.model \
    #                       --label 0 \
    #                       --remove-stopwords \
    #                       --keep-sequence
        echo 'Import new data for inferring.'
    else
        echo 'Error Process'
    fi
else
    echo 'Import file already exist, nothing to do.'
fi
echo $( date +%T )' :: Imported.'


# Train model
if [ $PROCESS == 'train' ]
then
    echo $( date +%T )' :: Start training dataset...'
    #mallet train-topics --input $OUTPUT/import.model \
    #                    --num-topics $TOPICS \
    #                    --optimize-interval $INTERVAL \
    #                    --optimize-burn-in $BURNIN \
    #                    --random-seed $SEED1 \
    #                    --num-threads 8 \
    #                    --num-iterations $ITERATION \
    #                    --output-model $OUTPUT/lda.model \
    #                    --output-doc-topics $OUTPUT/docTopics.txt \
    #                    --output-topic-keys $OUTPUT/topicKeys.txt \
    #                    --num-top-words 10 \
    #                    --diagnostics-file $OUTPUT/diagnostics.xml \
    #                    --output-state $OUTPUT/state.gz \
    #                    --inferencer-filename $OUTPUT/inferencer.model
    #                    #--topic-word-weights-file $OUTPUT/topicWordWeigts.txt \
    echo $( date +%T )' :: Trained.'
fi


# Infer topics
if [ $PROCESS == 'infer' ]
then
    echo $( date +%T )' :: Start infering dataset...'
    #mallet infer-topics --inferencer $INFER \
    #                    --input $OUTPUT/import.model \
    #                    --random-seed $SEED2 \
    #                    --output-doc-topics $OUTPUT/docTopicsInfer.txt
    echo $( date +%T )' :: Inferred.'
fi

