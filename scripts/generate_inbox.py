import os
import csv
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from uuid import uuid4

# -----------------------------
# Config
# -----------------------------
@dataclass
class Config:
    output_dir: str = "./inbox"
    start_date: date = date(2025, 10, 1)   # batch_date inicial
    num_days: int = 10                      # quantos dias de arquivos
    num_merchants: int = 50
    apps_per_day: int = 120
    disb_rate: float = 0.55                 # % de APPROVED que viram disbursement
    pays_per_disb: int = 5
    no_header_every_n_days: int = 3         # a cada N dias, omite header em merchants/disbursements (simula variação)
    duplicate_rate: float = 0.03            # % de duplicatas por arquivo
    invalid_rate: float = 0.01              # % de linhas inválidas
    broken_ref_rate: float = 0.01           # % de refs quebradas
    late_arrival_rate: float = 0.08         # % de eventos com date backdated (chega hoje, data do evento no passado)
    seed: int = 42

MERCH_HEADERS = ["merchant_id","business_name","industry_code","state_code","annual_revenue","employees_count","risk_score","onboarding_date"]
APP_HEADERS   = ["application_id","merchant_id","application_date","requested_amount","loan_purpose","application_status","credit_score","processing_time"]
DISB_HEADERS  = ["disbursement_id","application_id","merchant_id","disbursed_amount","disbursement_date","interest_rate","term_months","repayment_schedule"]
PAY_HEADERS   = ["payment_id","disbursement_id","merchant_id","payment_date","payment_amount","payment_method","is_scheduled","days_from_due","processing_timestamp"]

STATES = ["CA","TX","FL","NY","IL","WA","MA","GA","CO","AZ"]
INDUSTRIES = ["42310","44512","54161","62120","72251","33411","81111","53111"]
PURPOSES = ["INVENTORY","WORKING_CAPITAL","EXPANSION","EQUIPMENT","PAYROLL"]
SCHEDULES = ["DAILY","WEEKLY","MONTHLY"]
PAY_METHODS = ["ACH","CARD","CHECK","WIRE"]

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def write_csv(path: str, headers: list[str], rows: list[list], include_header: bool = True):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if include_header:
            w.writerow(headers)
        w.writerows(rows)

def fmt_date(d: date) -> str:
    return d.isoformat()

def fmt_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def chance(p: float) -> bool:
    return random.random() < p

def clamp(x, lo, hi):
    return max(lo, min(hi, x))

def gen_merchants(cfg: Config):
    merchants = []
    for i in range(cfg.num_merchants):
        mid = str(uuid4())
        name = f"Merchant {i:04d}"
        industry = random.choice(INDUSTRIES)
        state = random.choice(STATES)
        revenue = round(random.uniform(50_000, 5_000_000), 2)
        employees = random.randint(1, 250)
        risk = round(clamp(random.random(), 0, 1), 2)
        onboard = cfg.start_date - timedelta(days=random.randint(30, 900))
        merchants.append({
            "merchant_id": mid,
            "business_name": name,
            "industry_code": industry,
            "state_code": state,
            "annual_revenue": f"{revenue:.2f}",
            "employees_count": str(employees),
            "risk_score": f"{risk:.2f}",
            "onboarding_date": fmt_date(onboard),
        })
    return merchants

def sample_merch(merchants):
    return random.choice(merchants)

def mutate_merchant(m: dict):
    # muda alguma coisa pra gerar SCD2
    m2 = dict(m)
    if random.random() < 0.5:
        # drift de risk_score
        r = float(m2["risk_score"])
        r = clamp(r + random.uniform(-0.10, 0.10), 0, 1)
        m2["risk_score"] = f"{r:.2f}"
    else:
        rev = float(m2["annual_revenue"])
        rev = max(1.0, rev * random.uniform(0.95, 1.15))
        m2["annual_revenue"] = f"{rev:.2f}"
    return m2

def main(cfg: Config):
    random.seed(cfg.seed)
    ensure_dir(cfg.output_dir)

    # Base merchants pool
    merchants = gen_merchants(cfg)
    merchant_by_id = {m["merchant_id"]: m for m in merchants}

    # Carry-over maps to create duplicates/updates across days
    carry_apps = {}    # application_id -> row
    carry_pays = {}    # payment_id -> row

    all_app_ids = []
    all_disb_ids = []

    for day_idx in range(cfg.num_days):
        batch_date = cfg.start_date + timedelta(days=day_idx)
        bd = fmt_date(batch_date)

        include_header_merch = not (cfg.no_header_every_n_days and (day_idx % cfg.no_header_every_n_days == 1))
        include_header_disb  = not (cfg.no_header_every_n_days and (day_idx % cfg.no_header_every_n_days == 2))

        # -----------------------------
        # Merchants file (snapshot-ish + occasional changes)
        # -----------------------------
        merch_rows = []
        # snapshot do dia: todos merchants, mas com algumas mutações pra simular updates
        for m in merchants:
            row = m
            if chance(0.08):  # ~8% mudam por dia
                row = mutate_merchant(m)
                merchant_by_id[m["merchant_id"]] = row
            merch_rows.append([
                row["merchant_id"], row["business_name"], row["industry_code"], row["state_code"],
                row["annual_revenue"], row["employees_count"], row["risk_score"], row["onboarding_date"]
            ])

        # duplicatas e inválidos em merchants
        if merch_rows and chance(cfg.duplicate_rate):
            merch_rows.append(random.choice(merch_rows))
        if chance(cfg.invalid_rate):
            merch_rows.append(["not-a-uuid","Bad Merchant","ABCDE","C","-1","x","1.50","not-a-date"])

        write_csv(os.path.join(cfg.output_dir, f"merchants_{bd}.csv"), MERCH_HEADERS, merch_rows, include_header=include_header_merch)

        # -----------------------------
        # Applications file
        # -----------------------------
        apps_rows = []

        # às vezes reenvia/atualiza uma aplicação antiga (duplicate across days)
        if carry_apps and chance(0.25):
            old_id, old_row = random.choice(list(carry_apps.items()))
            # atualiza status/processing_time
            updated = old_row.copy()
            updated[5] = "APPROVED" if updated[5] == "PENDING" else updated[5]
            updated[7] = fmt_ts(datetime.combine(batch_date, datetime.min.time()) + timedelta(hours=2, minutes=random.randint(0, 59)))
            apps_rows.append(updated)

        for _ in range(cfg.apps_per_day):
            app_id = str(uuid4())
            m = sample_merch(list(merchant_by_id.values()))
            # late arrival: application_date pode ser antes do batch_date
            app_date = batch_date - timedelta(days=random.randint(0, 10) if chance(cfg.late_arrival_rate) else random.randint(0, 2))
            requested = round(random.uniform(5_000, 250_000), 2)
            status = random.choices(["PENDING","APPROVED","REJECTED"], weights=[0.25, 0.55, 0.20], k=1)[0]
            credit = random.randint(300, 850)
            proc_ts = datetime.combine(batch_date, datetime.min.time()) + timedelta(hours=2, minutes=random.randint(0, 59))
            row = [
                app_id,
                m["merchant_id"],
                fmt_date(app_date),
                f"{requested:.2f}",
                random.choice(PURPOSES),
                status,
                str(credit),
                fmt_ts(proc_ts)
            ]
            apps_rows.append(row)
            carry_apps[app_id] = row
            all_app_ids.append(app_id)

            # duplicata dentro do arquivo
            if chance(cfg.duplicate_rate):
                apps_rows.append(row)

        # inválido em applications
        if chance(cfg.invalid_rate):
            apps_rows.append(["bad-app","", "not-a-date","-5","OTHER","UNKNOWN","999","not-a-ts"])

        write_csv(os.path.join(cfg.output_dir, f"applications_{bd}.csv"), APP_HEADERS, apps_rows, include_header=True)

        # -----------------------------
        # Disbursements file
        # -----------------------------
        disb_rows = []

        # Pega algumas apps APPROVED do dia (e de dias anteriores, já que pode ter late-arrival)
        approved_apps = [r for r in apps_rows if len(r) >= 6 and r[5] == "APPROVED"]
        random.shuffle(approved_apps)

        num_disb = int(len(approved_apps) * cfg.disb_rate)
        for i in range(num_disb):
            app_row = approved_apps[i]
            app_id = app_row[0]
            merch_id = app_row[1]

            disb_id = str(uuid4())
            # late arrival: disbursement_date pode ser antes do batch_date
            disb_date = batch_date - timedelta(days=random.randint(1, 20) if chance(cfg.late_arrival_rate) else random.randint(0, 2))
            amount = float(app_row[3])  # use requested_amount como base
            amount = round(amount * random.uniform(0.85, 1.0), 2)

            # referência quebrada de propósito em poucos casos
            if chance(cfg.broken_ref_rate):
                app_id_used = str(uuid4())  # app inexistente
            else:
                app_id_used = app_id

            row = [
                disb_id,
                app_id_used,
                merch_id,
                f"{amount:.2f}",
                fmt_date(disb_date),
                f"{random.uniform(0.08, 0.25):.4f}",
                str(random.choice([6, 9, 12, 18])),
                random.choice(SCHEDULES)
            ]
            disb_rows.append(row)
            all_disb_ids.append(disb_id)

            # duplicata dentro do arquivo
            if chance(cfg.duplicate_rate):
                disb_rows.append(row)

        # inválido em disbursements
        if chance(cfg.invalid_rate):
            disb_rows.append(["bad-disb", str(uuid4()), str(uuid4()), "0", "not-a-date", "-0.1", "0", "YEARLY"])

        write_csv(os.path.join(cfg.output_dir, f"disbursements_{bd}.csv"), DISB_HEADERS, disb_rows, include_header=include_header_disb)

        # -----------------------------
        # Payments file
        # -----------------------------
        pay_rows = []

        # às vezes reenvia/atualiza um payment antigo (duplicate across days)
        if carry_pays and chance(0.25):
            old_pid, old_row = random.choice(list(carry_pays.items()))
            updated = old_row.copy()
            # muda amount e processing_timestamp pra ser "mais recente"
            updated[4] = f"{max(1.0, float(updated[4]) * random.uniform(0.95, 1.05)):.2f}"
            updated[8] = fmt_ts(datetime.combine(batch_date, datetime.min.time()) + timedelta(hours=10, minutes=random.randint(0, 59)))
            pay_rows.append(updated)

        # pagamentos para alguns disbursements
        for disb_row in disb_rows:
            disb_id = disb_row[0]
            merch_id = disb_row[2]
            disb_date = date.fromisoformat(disb_row[4])

            # gera N pagamentos por disbursement
            for k in range(cfg.pays_per_disb):
                pid = str(uuid4())
                pay_date = disb_date + timedelta(days=(k+1)*7)  # semanal como exemplo
                amount = round(float(disb_row[3]) / cfg.pays_per_disb * random.uniform(0.9, 1.1), 2)
                method = random.choice(PAY_METHODS)
                is_sched = random.choice(["TRUE","FALSE"])

                # days_from_due: mistura de on-time, early, late e alguns bem atrasados (para proxy de default)
                if chance(0.03):
                    days_from_due = random.randint(30, 60)  # "default proxy"
                else:
                    days_from_due = random.choice([0, 0, 0, -2, -1, 1, 3, 7, 12])

                proc_ts = datetime.combine(batch_date, datetime.min.time()) + timedelta(hours=9, minutes=random.randint(0, 59))

                # referência quebrada de propósito
                disb_id_used = disb_id if not chance(cfg.broken_ref_rate) else str(uuid4())

                row = [
                    pid,
                    disb_id_used,
                    merch_id,
                    fmt_date(pay_date),
                    f"{max(1.0, amount):.2f}",
                    method,
                    is_sched,
                    str(days_from_due),
                    fmt_ts(proc_ts)
                ]
                pay_rows.append(row)
                carry_pays[pid] = row

                # duplicata dentro do arquivo
                if chance(cfg.duplicate_rate):
                    pay_rows.append(row)

        # inválido em payments
        if chance(cfg.invalid_rate):
            pay_rows.append(["bad-pay", str(uuid4()), "", "not-a-date", "-1", "CASH", "MAYBE", "x", "not-a-ts"])

        write_csv(os.path.join(cfg.output_dir, f"payments_{bd}.csv"), PAY_HEADERS, pay_rows, include_header=True)

    print(f"✅ Gerado {cfg.num_days} dias de arquivos em: {os.path.abspath(cfg.output_dir)}")
    print("Exemplos:")
    for fn in sorted(os.listdir(cfg.output_dir))[:8]:
        print(" -", fn)


if __name__ == "__main__":
    cfg = Config(
        output_dir="./inbox",
        start_date=date(2025, 10, 1),
        num_days=10,
        num_merchants=50,
        apps_per_day=120,
        pays_per_disb=5,
        seed=42
    )
    main(cfg)
