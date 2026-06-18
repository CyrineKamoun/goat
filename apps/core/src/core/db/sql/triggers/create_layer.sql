CREATE OR REPLACE FUNCTION accounts.create_layer_trigger()
RETURNS TRIGGER AS $$
DECLARE
    role_id UUID; 
BEGIN
  -- Get the role_id of the user
  SELECT id 
  INTO role_id 
  FROM accounts.role 
  WHERE name = 'layer-owner';

  -- Insert a new row into accounts.layer_user table when a row is added to customer.layer table
  INSERT INTO accounts.layer_user (layer_id, user_id, role_id)
  VALUES (NEW.id, NEW.user_id, role_id);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER add_layer_user_trigger
AFTER INSERT ON customer.layer
FOR EACH ROW
EXECUTE FUNCTION accounts.create_layer_trigger();
