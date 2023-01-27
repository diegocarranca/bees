#' @title
#' Distribuir orcamento para agencias
#'
#' @description
#' Distribui proporcionalmente o valor orcado para a rede de agencias
#' em funcao do total do ano ou do ultimo mes.
#'
#' @export
#'
#' @param caminho String. O caminho do orcamento realizado do ano anterior,
#' incluindo a extensao (.txt).
#' @param tipo String. "resultado" ou "saldo".
#' @param previsao Numeric. Vetor das previsoes para o ano seguinte.
#'
#' @details O vetor de previsoes deve ter 12 valores, um para cada mes.
#' O arquivo a ser importado deve ser baixado do PPO, aberto e convertido
#' para xlsx.
#'
#' @examples
#' \dontrun{
#' # vetor de previsao
#' previsao = rep(1000, 12)
#' # obter dataframe
#' orcamento_ag(
#' caminho = "\\path\\to\\file.xlsx"),
#' tipo = "saldo",
#' previsao = previsao
#' }
#'
#' @import dplyr
#'

orcamento_ag = function(
    caminho,
    tipo,
    previsao) {

    # validando argumentos
    match.arg(tipo, choices = c("resultado", "saldo"))

    # importando e limpando tabela
    ano_anterior = janitor::remove_empty(
        readxl::read_excel(caminho, skip = 9)
    )

    # criando novo dataframe
    novo = ano_anterior %>%
        select(CID, `Descrição`)

    novas_colunas = format(
        seq.Date(
            as.Date("2021-01-01"),
            length.out = 12,
            by = "month"
        ),
        "%B"
    )

    novo[, novas_colunas] = NA

    # calculando proporção
    if (tipo == "resultado") {
        proporcao = ano_anterior$Total / tail(ano_anterior$Total, 1)
    } else {
        proporcao = ano_anterior[, ncol(ano_anterior) - 1, drop = TRUE] /
            tail(ano_anterior[, ncol(ano_anterior) - 1, drop = TRUE], 1)
    }

    # criando coluna de proporção no novo dataframe
    novo$proporcao = proporcao

    # preenchendo meses
    novo[, which(colnames(novo) %in% novas_colunas)] = sapply(
        previsao, function(x) {
            x * novo$proporcao
        }
    )

    # PRONTO CARALHO
    return(novo)
}
