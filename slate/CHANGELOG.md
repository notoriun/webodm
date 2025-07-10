# Changelog

## Versão 1.0.31

*10-07-25*

- Removidas pastas de pacotes para diminuir o tamanho da imagem do docker 

## Versão 1.0.30

*10-07-25*

- Separado dockerfile em 2 para melhorar a performance do build
- ⁠⁠Melhorado tratamento e rastreabilidade de erros de tasks de upload de imagens no celery, corrigindo o erro de chaves no FileDict e especificado melhor os erros no logger, das tasks de upload no celery

## Versão 1.0.29

*27-06-25*

- ⁠Adicionado endpoint /upload-auth, que requer token de autenticação para poder consumir
- ⁠⁠Adicionada configuração de TLS no broker do redis
- ⁠⁠Melhorado tratamento de erros nos endpoints de tasks do celery

## Versão 1.0.28

*27-05-25*

- Adiciona funcionalidade de controle de retomada de upload de imagens caso um worker caia no meio.

## Versão 1.0.26

*08-05-25*

- Corrige continuidade de tarefas do celery

## Versão 1.0.25

*08-05-25*

- Corrige nome do path ao baixar arquivos do S3

## Versão 1.0.24

*08-05-25*

- Corrige duplicação de arquivos baixados do S3.
- Correge o local de armazenamento de arquivos ao baixar imagens do S3 pela segunda vez.

## Versão 1.0.23

*28-04-25*

- Corrige problema ao realizar medição que não buscava arquivos necessários do S3 se não estivesse no cache.

## Versão 1.0.22

*09-04-25*

- Busca dados do DynamoDB para exibição de fotos e vídeos no timeline.
- Corrige erros no gerenciamento do cache de arquivos de fotos, imagens e ortofotos.


## Versão 1.0.21

*04-04-25*

- Altera forma de registro de arquivos do S3 evitando duplicação destes arquivos no bucket.


## Versão 1.0.20

*25-03-25*

- Adiciona extração de lat e lon em arquivos de vídeo em mais de um metadata.
- Altera forma de recebimento de ortofotos evitando duplicidade e arquivos órfãos.

## Versão 1.0.18

*24-03-25*

- Refactory do controle de recebimento de arquivos (foto, vídeo, foto 360)
- Adiciona status "60 - WAITING_NODE" a tarefas que não possuem um nó disponível para execução.
- Melhoria da limpeza de nodes com tarefas zumbis.

## Versão 1.0.17

*20-03-25*

- Aprimoramento do controle de transações.
- Adiciona novos logs para depuração.

## Versão 1.0.16

*19-03-25*

- Corrige problemas de paralelismo na sincronização do S3 e banco de dados.
- Corrige problemas de tarefas (tasks) zumbis no servidor.

## Versão 1.0.15

*17-03-25*

- Corrige problemas de sincronização do cache REDIS com S3.
- Corrige problemas de sincronização do postgres com S3 (available_assets).
- Corrige erro de duplicação de arquivos de foto 360.

## Versão 1.0.14

*14-03-25*

- Adiciona possibilidade de exibição de mais de um arquivo de foto 360.
- Corrige problemas de sincronização com S3.


## Versão 1.0.13

*13-03-25*

- Corrige erro ao enviar arquivos (assets) para S3


## Version 1.4.0

*November 24, 2016*

- Upgrade Middleman and Rouge gems, should hopefully solve a number of bugs
- Update some links in README
- Fix broken Vagrant startup script
- Fix some problems with deploy.sh help message
- Fix bug with language tabs not hiding properly if no error
- Add `!default` to SASS variables
- Fix bug with logo margin
- Bump tested Ruby versions in .travis.yml

## Version 1.3.3

*June 11, 2016*

Documentation and example changes.

## Version 1.3.2

*February 3, 2016*

A small bugfix for slightly incorrect background colors on code samples in some cases.

## Version 1.3.1

*January 31, 2016*

A small bugfix for incorrect whitespace in code blocks.

## Version 1.3

*January 27, 2016*

We've upgraded Middleman and a number of other dependencies, which should fix quite a few bugs.

Instead of `rake build` and `rake deploy`, you should now run `bundle exec middleman build --clean` to build your server, and `./deploy.sh` to deploy it to Github Pages.

## Version 1.2

*June 20, 2015*

**Fixes:**

- Remove crash on invalid languages
- Update Tocify to scroll to the highlighted header in the Table of Contents
- Fix variable leak and update search algorithms
- Update Python examples to be valid Python
- Update gems
- More misc. bugfixes of Javascript errors
- Add Dockerfile
- Remove unused gems
- Optimize images, fonts, and generated asset files
- Add chinese font support
- Remove RedCarpet header ID patch
- Update language tabs to not disturb existing query strings

## Version 1.1

*July 27, 2014*

**Fixes:**

- Finally, a fix for the redcarpet upgrade bug

## Version 1.0

*July 2, 2014*

[View Issues](https://github.com/tripit/slate/issues?milestone=1&state=closed)

**Features:**

- Responsive designs for phones and tablets
- Started tagging versions

**Fixes:**

- Fixed 'unrecognized expression' error
- Fixed #undefined hash bug
- Fixed bug where the current language tab would be unselected
- Fixed bug where tocify wouldn't highlight the current section while searching
- Fixed bug where ids of header tags would have special characters that caused problems
- Updated layout so that pages with disabled search wouldn't load search.js
- Cleaned up Javascript
