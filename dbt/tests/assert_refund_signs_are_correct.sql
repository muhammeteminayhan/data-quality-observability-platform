-- A refund reduces the balance; a settlement increases it.
--
-- A positive refund or a negative settlement is a sign-handling bug that
-- would silently invert revenue.
select payment_id, payment_type, payment_value
from {{ ref('stg_payments') }}
where (payment_type = 'refund' and payment_value >= 0)
   or (payment_type <> 'refund' and payment_value <= 0)
